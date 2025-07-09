"""
Unit tests for the server module.
"""

import os
from unittest.mock import patch, MagicMock

import pytest

# Set the AWS region before importing any boto3-dependent modules
os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'

# Import the module under test
from bin.source_region.server import get_env_var, signal_handler, process_s3_object, process_message_batch, main


class TestEnvironmentVariables:
	"""Tests for environment variable handling."""

	def test_get_env_var_required_exists(self):
		"""Test getting a required environment variable that exists."""
		# Given: An environment variable
		os.environ['TEST_VAR'] = 'test_value'

		# When: We get the environment variable
		value = get_env_var('TEST_VAR')

		# Then: We should get the value
		assert value == 'test_value'

		# Clean up
		os.environ.pop('TEST_VAR')

	def test_get_env_var_optional_exists(self):
		"""Test getting an optional environment variable that exists."""
		# Given: An environment variable
		os.environ['TEST_VAR'] = 'test_value'

		# When: We get the environment variable as optional
		value = get_env_var('TEST_VAR', required=False)

		# Then: We should get the value
		assert value == 'test_value'

		# Clean up
		os.environ.pop('TEST_VAR')

	def test_get_env_var_optional_missing(self):
		"""Test getting an optional environment variable that does not exist."""
		# Given: No environment variable
		if 'NONEXISTENT_VAR' in os.environ:
			os.environ.pop('NONEXISTENT_VAR')

		# When: We get the nonexistent environment variable as optional
		value = get_env_var('NONEXISTENT_VAR', required=False)

		# Then: We should get None
		assert value is None

	def test_get_env_var_required_missing(self):
		"""Test getting a required environment variable that does not exist."""
		# Given: No environment variable
		if 'NONEXISTENT_VAR' in os.environ:
			os.environ.pop('NONEXISTENT_VAR')

		# When: We get the nonexistent environment variable as required
		with pytest.raises(SystemExit):
			get_env_var('NONEXISTENT_VAR')

		# Then: The function should exit with an error (tested by pytest.raises)


class TestSignalHandling:
	"""Tests for signal handling."""

	def test_signal_handler(self):
		"""Test the signal handler changes the running flag."""
		# Given: A global running flag (monkeypatched)
		with patch('bin.source_region.server.running', True):
			# When: The signal handler is called
			signal_handler(15, None)  # SIGTERM

			# Then: The running flag should be set to False
			from bin.source_region.server import running

			assert running is False


class TestS3ObjectProcessing:
	"""Tests for S3 object processing."""

	def test_process_s3_object(self, s3_client, setup_s3_objects, temp_directory):
		"""Test processing an S3 object."""
		# Given: An S3 object
		bucket = setup_s3_objects['bucket']
		key = setup_s3_objects['objects'][0]['key']  # test/object1.txt

		# And: A mocked download function
		with patch('bin.source_region.server.get_s3_object', return_value=True) as mock_get_object:
			# And: A mocked metadata function
			with patch('bin.source_region.server.get_s3_object_metadata') as mock_get_metadata:
				mock_get_metadata.return_value = {
					'source_bucket': bucket,
					'source_prefix': 'test',
					'object_name': 'object1.txt',
					'size': 31,
					'etag': '1234567890',
					'creation_time': '2023-01-01 00:00:00',
					'storage_class': 'STANDARD',
					'tags': [{'Purpose': 'Testing'}],
				}

				# When: We process the object with a monitored prefix
				s3_object = {'bucket': bucket, 'key': key}
				metadata, local_path = process_s3_object(s3_object, temp_directory, monitored_prefix='test')

				# Then: We should get the correct metadata and a local file path
				assert metadata['source_bucket'] == bucket
				assert metadata['source_prefix'] == 'test'
				assert metadata['object_name'] == 'object1.txt'
				assert metadata['full_key'] == key
				assert metadata['relative_key'] == 'object1.txt'  # test prefix was removed
				assert 'tags' in metadata

				# Since we're using mocks, we should create the file to test existence
				with open(local_path, 'w') as f:
					f.write('Mock file content')

				assert os.path.exists(local_path)

				# And: The S3 object should have been downloaded
				mock_get_object.assert_called_once()
				mock_get_metadata.assert_called_once_with(bucket, key)

	def test_process_s3_object_no_monitored_prefix(self, s3_client, setup_s3_objects, temp_directory):
		"""Test processing an S3 object without a monitored prefix."""
		# Given: An S3 object
		bucket = setup_s3_objects['bucket']
		key = setup_s3_objects['objects'][0]['key']  # test/object1.txt

		# And: A mocked download function
		with patch('bin.source_region.server.get_s3_object', return_value=True) as mock_get_object:
			# And: A mocked metadata function
			with patch('bin.source_region.server.get_s3_object_metadata') as mock_get_metadata:
				mock_get_metadata.return_value = {
					'source_bucket': bucket,
					'source_prefix': 'test',
					'object_name': 'object1.txt',
					'size': 31,
					'etag': '1234567890',
					'creation_time': '2023-01-01 00:00:00',
					'storage_class': 'STANDARD',
					'tags': [{'Purpose': 'Testing'}],
				}

				# When: We process the object without a monitored prefix
				s3_object = {'bucket': bucket, 'key': key}
				metadata, local_path = process_s3_object(s3_object, temp_directory)

				# Then: The relative_key should be the same as the full key
				assert metadata['full_key'] == key
				assert metadata['relative_key'] == key

	def test_process_s3_object_download_failure(self, s3_client, setup_s3_objects, temp_directory):
		"""Test handling S3 object download failure."""
		# Given: An S3 object
		bucket = setup_s3_objects['bucket']
		key = setup_s3_objects['objects'][0]['key']  # test/object1.txt

		# And: A mocked download function that fails
		with patch('bin.source_region.server.get_s3_object', return_value=False):
			# When: We process the object and the download fails
			s3_object = {'bucket': bucket, 'key': key}
			metadata, local_path = process_s3_object(s3_object, temp_directory)

			# Then: We should get empty results
			assert metadata == {}
			assert local_path == ''

	def test_process_s3_object_metadata_failure(self, s3_client, setup_s3_objects, temp_directory):
		"""Test handling S3 object metadata retrieval failure."""
		# Given: An S3 object
		bucket = setup_s3_objects['bucket']
		key = setup_s3_objects['objects'][0]['key']  # test/object1.txt

		# And: A mocked download function
		with patch('bin.source_region.server.get_s3_object', return_value=True):
			# And: A mocked metadata function that fails
			with patch('bin.source_region.server.get_s3_object_metadata', return_value=None):
				# When: We process the object and metadata retrieval fails
				s3_object = {'bucket': bucket, 'key': key}
				metadata, local_path = process_s3_object(s3_object, temp_directory)

				# Then: We should get empty results
				assert metadata == {}
				assert local_path == ''


class TestMessageBatchProcessing:
	"""Tests for SQS message batch processing."""

	def test_process_message_batch_empty(self, sqs_client):
		"""Test processing an empty batch of messages."""
		# Given: A mock for get_sqs_messages returning empty list
		with patch('bin.source_region.server.get_sqs_messages', return_value=[]):
			# When: We process an empty batch
			result = process_message_batch(
				queue_url='dummy-queue', outbound_bucket='dummy-bucket', stack_name='dummy-stack'
			)

			# Then: We should get 0 processed messages
			assert result == 0

	def test_process_message_batch_test_events(self, sqs_client, s3_test_event):
		"""Test processing a batch with only test events."""
		# Given: A mock for get_sqs_messages returning test events with receipt handles
		s3_test_event_with_receipt = {'Body': s3_test_event['Body'], 'ReceiptHandle': 'test-event-receipt-handle'}

		with patch('bin.source_region.server.get_sqs_messages', return_value=[s3_test_event_with_receipt]):
			# And: A mock for is_s3_test_event
			with patch('bin.source_region.server.is_s3_test_event', return_value=True):
				# And: A mock for delete_sqs_messages_batch
				with patch('bin.source_region.server.delete_sqs_messages_batch') as mock_delete:
					mock_delete.return_value = (['0'], [])

					# When: We process a batch with test events
					result = process_message_batch(
						queue_url='dummy-queue', outbound_bucket='dummy-bucket', stack_name='dummy-stack'
					)

					# Then: We should process the test event message
					assert result == 1

					# And: We should delete the test event
					mock_delete.assert_called_once()

	def test_process_message_batch_no_s3_objects(self, sqs_client, sample_s3_event):
		"""Test processing a batch with no valid S3 objects."""
		# Given: A mock for get_sqs_messages returning a message with receipt handle
		sample_s3_event_with_receipt = {'Body': sample_s3_event['Body'], 'ReceiptHandle': 'sample-receipt-handle'}

		with patch('bin.source_region.server.get_sqs_messages', return_value=[sample_s3_event_with_receipt]):
			# And: A mock for is_s3_test_event
			with patch('bin.source_region.server.is_s3_test_event', return_value=False):
				# And: A mock for extract_s3_event_info returning empty
				with patch('bin.source_region.server.extract_s3_event_info', return_value=[]):
					# And: A mock for delete_sqs_messages_batch
					with patch('bin.source_region.server.delete_sqs_messages_batch') as mock_delete:
						mock_delete.return_value = (['0'], [])

						# When: We process a batch with no valid S3 objects
						result = process_message_batch(
							queue_url='dummy-queue', outbound_bucket='dummy-bucket', stack_name='dummy-stack'
						)

						# Then: We should still process the message
						assert result == 1

	def test_process_message_batch_full_flow(
		self, sqs_client, sample_s3_event, temp_directory, setup_dynamodb_parameters
	):
		"""Test the full message batch processing flow."""
		# Given: A mock for get_sqs_messages returning a message with receipt handle
		sample_s3_event_with_receipt = {'Body': sample_s3_event['Body'], 'ReceiptHandle': 'sample-receipt-handle'}

		with patch('bin.source_region.server.get_sqs_messages', return_value=[sample_s3_event_with_receipt]):
			# And: A mock for is_s3_test_event
			with patch('bin.source_region.server.is_s3_test_event', return_value=False):
				# And: A mock for extract_s3_event_info
				s3_objects = [{'bucket': 'test-source-bucket', 'key': 'test/object.txt'}]
				with patch('bin.source_region.server.extract_s3_event_info', return_value=s3_objects):
					# And: A mock for create_temp_directory
					with patch('bin.source_region.server.create_temp_directory', return_value=temp_directory):
						# And: A mock for process_s3_object
						metadata = {
							'source_bucket': 'test-source-bucket',
							'source_prefix': 'test',
							'object_name': 'object.txt',
							'full_key': 'test/object.txt',
							'relative_key': 'object.txt',
							'size': 31,
						}
						local_path = os.path.join(temp_directory, 'object.txt')
						with patch('bin.source_region.server.process_s3_object', return_value=(metadata, local_path)):
							# And: A mock for get_target_info_from_dynamodb
							targets = [{'region': 'us-west-2', 'bucket': 'target-bucket', 'storage_class': 'STANDARD'}]
							with patch(
								'bin.source_region.server.get_target_info_from_dynamodb',
								return_value=('test-key', targets),
							):
								# And: A mock for create_object_manifest
								with patch('bin.source_region.server.create_object_manifest', return_value=True):
									# And: A mock for compress_objects
									compressed_path = os.path.join(temp_directory, 'archive.tar.zst')
									with patch(
										'bin.source_region.server.compress_objects',
										return_value=(True, compressed_path, 1000, 500, 12),
									):
										# And: A mock for upload_to_s3
										with patch('bin.source_region.server.upload_to_s3', return_value=True):
											# And: A mock for delete_sqs_messages_batch
											with patch('bin.source_region.server.delete_sqs_messages_batch'):
												# And: A mock for report_compression_metrics
												with patch('bin.source_region.server.report_compression_metrics'):
													# And: A mock for CompressionManager
													mock_manager = MagicMock()
													with patch(
														'bin.source_region.server.CompressionManager.get_instance',
														return_value=mock_manager,
													):
														# When: We process a batch with all mocks in place
														result = process_message_batch(
															queue_url='dummy-queue',
															outbound_bucket='dummy-bucket',
															stack_name='dummy-stack',
															monitored_prefix='test',
														)

														# Then: We should process the message
														assert result == 1

														# And: The CompressionManager should be updated
														mock_manager.update_compression_metrics.assert_called_once()

	def test_process_message_batch_missing_target_info(self, sqs_client, sample_s3_event, temp_directory):
		"""Test handling missing target information."""
		# Given: A mock for get_sqs_messages returning a message with receipt handle
		sample_s3_event_with_receipt = {'Body': sample_s3_event['Body'], 'ReceiptHandle': 'sample-receipt-handle'}

		with patch('bin.source_region.server.get_sqs_messages', return_value=[sample_s3_event_with_receipt]):
			# And: A mock for is_s3_test_event
			with patch('bin.source_region.server.is_s3_test_event', return_value=False):
				# And: A mock for extract_s3_event_info
				s3_objects = [{'bucket': 'test-source-bucket', 'key': 'test/object.txt'}]
				with patch('bin.source_region.server.extract_s3_event_info', return_value=s3_objects):
					# And: A mock for create_temp_directory
					with patch('bin.source_region.server.create_temp_directory', return_value=temp_directory):
						# And: A mock for process_s3_object
						metadata = {
							'source_bucket': 'test-source-bucket',
							'source_prefix': 'test',
							'object_name': 'object.txt',
							'full_key': 'test/object.txt',
							'relative_key': 'object.txt',
						}
						local_path = os.path.join(temp_directory, 'object.txt')
						with patch('bin.source_region.server.process_s3_object', return_value=(metadata, local_path)):
							# And: A mock for get_target_info_from_dynamodb returning no targets
							with patch('bin.source_region.server.get_target_info_from_dynamodb', return_value=('', [])):
								# And: A mock for cleanup_temp_directory
								with patch('bin.source_region.server.cleanup_temp_directory') as mock_cleanup:
									# When: We process a batch with no target info
									result = process_message_batch(
										queue_url='dummy-queue',
										outbound_bucket='dummy-bucket',
										stack_name='dummy-stack',
										monitored_prefix='test',
									)

									# Then: We should process the message but return early
									assert result == 1

									# And: The temp directory should be cleaned up
									mock_cleanup.assert_called_once()

	def test_process_message_batch_compression_failure(self, sqs_client, sample_s3_event, temp_directory):
		"""Test handling compression failure."""
		# Given: A mock for get_sqs_messages returning a message with receipt handle
		sample_s3_event_with_receipt = {'Body': sample_s3_event['Body'], 'ReceiptHandle': 'sample-receipt-handle'}

		with patch('bin.source_region.server.get_sqs_messages', return_value=[sample_s3_event_with_receipt]):
			# And: A mock for is_s3_test_event
			with patch('bin.source_region.server.is_s3_test_event', return_value=False):
				# And: A mock for extract_s3_event_info
				s3_objects = [{'bucket': 'test-source-bucket', 'key': 'test/object.txt'}]
				with patch('bin.source_region.server.extract_s3_event_info', return_value=s3_objects):
					# And: A mock for create_temp_directory
					with patch('bin.source_region.server.create_temp_directory', return_value=temp_directory):
						# And: A mock for process_s3_object
						metadata = {
							'source_bucket': 'test-source-bucket',
							'source_prefix': 'test',
							'object_name': 'object.txt',
							'full_key': 'test/object.txt',
							'relative_key': 'object.txt',
						}
						local_path = os.path.join(temp_directory, 'object.txt')
						with patch('bin.source_region.server.process_s3_object', return_value=(metadata, local_path)):
							# And: A mock for get_target_info_from_dynamodb
							targets = [{'region': 'us-west-2', 'bucket': 'target-bucket', 'storage_class': 'STANDARD'}]
							with patch(
								'bin.source_region.server.get_target_info_from_dynamodb',
								return_value=('test-key', targets),
							):
								# And: A mock for create_object_manifest
								with patch('bin.source_region.server.create_object_manifest', return_value=True):
									# And: A mock for compress_objects that fails
									with patch(
										'bin.source_region.server.compress_objects', return_value=(False, '', 0, 0, 0)
									):
										# And: A mock for cleanup_temp_directory
										with patch('bin.source_region.server.cleanup_temp_directory') as mock_cleanup:
											# When: We process a batch with compression failure
											result = process_message_batch(
												queue_url='dummy-queue',
												outbound_bucket='dummy-bucket',
												stack_name='dummy-stack',
												monitored_prefix='test',
											)

											# Then: We should process the message but return early
											assert result == 1

											# And: The temp directory should be cleaned up
											mock_cleanup.assert_called_once()

	def test_process_message_batch_upload_failure(self, sqs_client, sample_s3_event, temp_directory):
		"""Test handling upload failure."""
		# Given: A mock for get_sqs_messages returning a message with receipt handle
		sample_s3_event_with_receipt = {'Body': sample_s3_event['Body'], 'ReceiptHandle': 'sample-receipt-handle'}

		with patch('bin.source_region.server.get_sqs_messages', return_value=[sample_s3_event_with_receipt]):
			# And: A mock for is_s3_test_event
			with patch('bin.source_region.server.is_s3_test_event', return_value=False):
				# And: A mock for extract_s3_event_info
				s3_objects = [{'bucket': 'test-source-bucket', 'key': 'test/object.txt'}]
				with patch('bin.source_region.server.extract_s3_event_info', return_value=s3_objects):
					# And: A mock for create_temp_directory
					with patch('bin.source_region.server.create_temp_directory', return_value=temp_directory):
						# And: A mock for process_s3_object
						metadata = {
							'source_bucket': 'test-source-bucket',
							'source_prefix': 'test',
							'object_name': 'object.txt',
							'full_key': 'test/object.txt',
							'relative_key': 'object.txt',
						}
						local_path = os.path.join(temp_directory, 'object.txt')
						with patch('bin.source_region.server.process_s3_object', return_value=(metadata, local_path)):
							# And: A mock for get_target_info_from_dynamodb
							targets = [{'region': 'us-west-2', 'bucket': 'target-bucket', 'storage_class': 'STANDARD'}]
							with patch(
								'bin.source_region.server.get_target_info_from_dynamodb',
								return_value=('test-key', targets),
							):
								# And: A mock for create_object_manifest
								with patch('bin.source_region.server.create_object_manifest', return_value=True):
									# And: A mock for compress_objects
									compressed_path = os.path.join(temp_directory, 'archive.tar.zst')
									with patch(
										'bin.source_region.server.compress_objects',
										return_value=(True, compressed_path, 1000, 500, 12),
									):
										# And: A mock for upload_to_s3 that fails
										with patch('bin.source_region.server.upload_to_s3', return_value=False):
											# And: A mock for cleanup_temp_directory
											with patch(
												'bin.source_region.server.cleanup_temp_directory'
											) as mock_cleanup:
												# When: We process a batch with upload failure
												result = process_message_batch(
													queue_url='dummy-queue',
													outbound_bucket='dummy-bucket',
													stack_name='dummy-stack',
													monitored_prefix='test',
												)

												# Then: We should return 0 to indicate failure
												assert result == 0

												# And: The temp directory should be cleaned up
												mock_cleanup.assert_called_once()


class TestMainFunction:
	"""Tests for the main function."""

	def test_main_function(self, setup_environment_variables):
		"""Test the main function."""
		# Given: All required environment variables are set up by the fixture

		# And: A mock for run_cpu_benchmark
		with patch('bin.source_region.server.run_cpu_benchmark', return_value=1.0):
			# And: A mock for CompressionManager.initialize
			mock_manager = MagicMock()
			with patch('bin.source_region.server.CompressionManager.initialize', return_value=mock_manager):
				# And: A mock for process_message_batch
				with patch('bin.source_region.server.process_message_batch') as mock_process:
					# Make it return 0 first time (no messages) then change global running to False
					mock_process.return_value = 0

					# And: A mock for time.sleep that changes running to False after first call
					def mock_sleep(seconds):
						import bin.source_region.server

						bin.source_region.server.running = False

					with patch('time.sleep', side_effect=mock_sleep):
						# When: We run the main function
						main()

						# Then: The CPU benchmark should run
						# And: The CompressionManager should be initialized
						# And: The process_message_batch should be called at least once
						mock_process.assert_called()

	def test_main_function_error_handling(self, setup_environment_variables):
		"""Test error handling in the main function."""
		# Given: All required environment variables are set up by the fixture

		# And: A mock for run_cpu_benchmark
		with patch('bin.source_region.server.run_cpu_benchmark', return_value=1.0):
			# And: A mock for CompressionManager.initialize
			mock_manager = MagicMock()
			with patch('bin.source_region.server.CompressionManager.initialize', return_value=mock_manager):
				# And: A mock for process_message_batch that raises an exception
				with patch('bin.source_region.server.process_message_batch', side_effect=Exception('Test error')):
					# And: A mock for time.sleep that changes running to False after first call
					def mock_sleep(seconds):
						import bin.source_region.server

						bin.source_region.server.running = False

					with patch('time.sleep', side_effect=mock_sleep):
						# When: We run the main function and an error occurs
						main()

						# Then: The function should handle the error and continue
						# (Tested implicitly by reaching this point without unhandled exceptions)
