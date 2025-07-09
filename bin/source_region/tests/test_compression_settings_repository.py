"""
Unit tests for the compression_settings_repository module.
"""

from unittest.mock import patch, MagicMock

# Import the module under test
from bin.source_region.utils.compression_settings_repository import CompressionSettingsRepository


class TestCompressionSettingsRepository:
	"""Tests for the CompressionSettingsRepository class."""

	def test_init_with_defaults(self):
		"""Test initialization with default values."""
		# Given: Environment variable for table name
		import os

		os.environ['COMPRESSION_SETTINGS_TABLE'] = 'test-table-from-env'

		# When: We initialize a repository with defaults
		with patch('boto3.client') as mock_boto_client:
			repo = CompressionSettingsRepository()

		# Then: It should use the environment variable for table name
		assert repo.table_name == 'test-table-from-env'

		# Clean up
		os.environ.pop('COMPRESSION_SETTINGS_TABLE', None)

	def test_init_with_custom_values(self):
		"""Test initialization with custom client and table name."""
		# Given: Custom DynamoDB client and table name
		mock_client = MagicMock()
		custom_table = 'custom-settings-table'

		# When: We initialize a repository with custom values
		repo = CompressionSettingsRepository(dynamodb_client=mock_client, table_name=custom_table)

		# Then: It should use the provided values
		assert repo.dynamodb == mock_client
		assert repo.table_name == custom_table

	def test_get_settings(self, dynamodb_client, setup_compression_settings):
		"""Test retrieving compression settings."""
		# Given: A repository and settings in DynamoDB
		repo = CompressionSettingsRepository(dynamodb_client=dynamodb_client, table_name='test-compression-settings')
		bucket_prefix = setup_compression_settings  # 'test-source-bucket/test/'

		# When: We get the compression settings
		settings = repo.get_settings(bucket_prefix)

		# Then: We should get the correct settings
		assert settings is not None
		assert settings['optimal_level'] == 12
		assert settings['version'] == 1
		assert len(settings['metrics_history']) == 2
		assert settings['total_processed'] == 100

	def test_get_settings_nonexistent(self, dynamodb_client, dynamodb_tables):
		"""Test retrieving nonexistent compression settings."""
		# Given: A repository
		repo = CompressionSettingsRepository(
			dynamodb_client=dynamodb_client, table_name=dynamodb_tables['settings_table']
		)
		nonexistent_prefix = 'nonexistent-bucket/prefix/'

		# When: We try to get nonexistent settings
		settings = repo.get_settings(nonexistent_prefix)

		# Then: The result should be None
		assert settings is None

	def test_get_settings_with_retry(self, dynamodb_client, setup_compression_settings):
		"""Test retrieving settings with retry logic."""
		# Given: A repository and settings in DynamoDB
		repo = CompressionSettingsRepository(dynamodb_client=dynamodb_client, table_name='test-compression-settings')
		bucket_prefix = setup_compression_settings  # 'test-source-bucket/test/'

		# When: We get the settings with retry
		settings = repo.get_settings_with_retry(bucket_prefix)

		# Then: We should get the correct settings
		assert settings is not None
		assert settings['optimal_level'] == 12
		assert settings['version'] == 1

		# And: The consistent read option should be used
		# This is hard to test with moto, but we can mock the dynamodb.get_item call
		with patch.object(repo.dynamodb, 'get_item') as mock_get_item:
			mock_get_item.return_value = {
				'Item': {
					'BucketPrefix': {'S': bucket_prefix},
					'OptimalLevel': {'N': '12'},
					'Version': {'N': '1'},
					'TotalProcessed': {'N': '100'},
					'LastUpdated': {'N': '1619712000'},
					'MetricsHistory': {'L': []},
				}
			}

			repo.get_settings_with_retry(bucket_prefix)

			# Verify that ConsistentRead=True was used
			mock_get_item.assert_called_with(
				TableName='test-compression-settings', Key={'BucketPrefix': {'S': bucket_prefix}}, ConsistentRead=True
			)

	def test_create_settings(self, dynamodb_client, dynamodb_tables):
		"""Test creating new compression settings."""
		# Given: A repository
		repo = CompressionSettingsRepository(
			dynamodb_client=dynamodb_client, table_name=dynamodb_tables['settings_table']
		)
		bucket_prefix = 'new-bucket/test/'
		optimal_level = 15

		# When: We create new settings
		success = repo.create_settings(bucket_prefix, optimal_level)

		# Then: The creation should be successful
		assert success is True

		# And: The settings should exist in DynamoDB
		response = dynamodb_client.get_item(
			TableName=dynamodb_tables['settings_table'], Key={'BucketPrefix': {'S': bucket_prefix}}
		)

		assert 'Item' in response
		assert response['Item']['OptimalLevel']['N'] == '15'
		assert response['Item']['TotalProcessed']['N'] == '0'
		assert 'Version' in response['Item']
		assert 'LastUpdated' in response['Item']
		assert response['Item']['MetricsHistory']['L'] == []

	def test_create_settings_error(self, dynamodb_client):
		"""Test handling errors when creating settings."""
		# Given: A repository with an invalid table name
		repo = CompressionSettingsRepository(dynamodb_client=dynamodb_client, table_name='nonexistent-table')
		bucket_prefix = 'new-bucket/test/'
		optimal_level = 15

		# When: We try to create settings and an error occurs
		success = repo.create_settings(bucket_prefix, optimal_level)

		# Then: The function should handle the error and return False
		assert success is False

	def test_update_settings(self, dynamodb_client, setup_compression_settings):
		"""Test updating compression settings."""
		# Given: A repository and existing settings
		repo = CompressionSettingsRepository(dynamodb_client=dynamodb_client, table_name='test-compression-settings')
		bucket_prefix = setup_compression_settings  # 'test-source-bucket/test/'

		# Get the current settings to get the version
		current_settings = repo.get_settings(bucket_prefix)
		current_version = current_settings['version']

		# New values to update
		new_level = 16
		new_metrics = [
			{
				'M': {
					'Level': {'N': '16'},
					'OriginalSize': {'N': '2000'},
					'CompressedSize': {'N': '800'},
					'ProcessingTime': {'N': '3'},
					'NumRegions': {'N': '2'},
					'Timestamp': {'N': '1619712100'},
					'CostBenefitScore': {'N': '0.9'},
				}
			}
		]

		# When: We update the settings
		success, new_version = repo.update_settings(bucket_prefix, new_level, new_metrics, current_version)

		# Then: The update should be successful
		assert success is True
		assert new_version > current_version

		# And: The settings should be updated in DynamoDB
		updated_settings = repo.get_settings(bucket_prefix)
		assert updated_settings['optimal_level'] == 16
		assert len(updated_settings['metrics_history']) == 1  # We replaced the history
		assert updated_settings['version'] == new_version

	def test_update_settings_version_conflict(self, dynamodb_client, setup_compression_settings):
		"""Test handling version conflicts when updating settings."""
		# Given: A repository and existing settings
		repo = CompressionSettingsRepository(dynamodb_client=dynamodb_client, table_name='test-compression-settings')
		bucket_prefix = setup_compression_settings  # 'test-source-bucket/test/'

		# When: We update with an outdated version
		old_version = 0  # Definitely outdated
		success, new_version = repo.update_settings(bucket_prefix, 16, [], old_version)

		# Then: The update should fail due to version conflict
		assert success is False
		assert new_version == old_version  # Returns the old version when there's a conflict

	def test_update_settings_nonexistent(self, dynamodb_client, dynamodb_tables):
		"""Test updating nonexistent settings."""
		# Given: A repository
		repo = CompressionSettingsRepository(
			dynamodb_client=dynamodb_client, table_name=dynamodb_tables['settings_table']
		)
		nonexistent_prefix = 'nonexistent-bucket/prefix/'

		# When: We try to update nonexistent settings
		success, new_version = repo.update_settings(nonexistent_prefix, 16, [], 1)

		# Then: The update should fail
		assert success is False
		assert new_version == 1  # Returns the provided version when item doesn't exist

	def test_update_settings_error(self, dynamodb_client):
		"""Test handling errors when updating settings."""
		# Given: A repository with an invalid table name
		repo = CompressionSettingsRepository(dynamodb_client=dynamodb_client, table_name='nonexistent-table')
		bucket_prefix = 'test-bucket/test/'

		# When: We try to update settings and an error occurs
		success, new_version = repo.update_settings(bucket_prefix, 16, [], 1)

		# Then: The function should handle the error and return False
		assert success is False
		assert new_version == 1  # Returns the current_version on error, not None
