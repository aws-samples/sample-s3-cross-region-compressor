"""
Unit tests for the parameters_repository module.
"""

import os
from unittest.mock import patch, MagicMock

# Import the module under test
from bin.source_region.utils.parameters_repository import ParametersRepository


class TestParametersRepository:
	"""Tests for the ParametersRepository class."""

	def test_init_with_defaults(self):
		"""Test initialization with default values."""
		# Given: Environment variable for table name
		os.environ['REPLICATION_PARAMETERS_TABLE'] = 'test-table-from-env'

		# When: We initialize a repository with defaults
		with patch('boto3.client') as mock_boto_client:
			repo = ParametersRepository()

		# Then: It should use the environment variable for table name
		assert repo.table_name == 'test-table-from-env'

		# Clean up
		os.environ.pop('REPLICATION_PARAMETERS_TABLE', None)

	def test_init_with_custom_values(self):
		"""Test initialization with custom client and table name."""
		# Given: Custom DynamoDB client and table name
		mock_client = MagicMock()
		custom_table = 'custom-table-name'

		# When: We initialize a repository with custom values
		repo = ParametersRepository(dynamodb_client=mock_client, table_name=custom_table)

		# Then: It should use the provided values
		assert repo.dynamodb == mock_client
		assert repo.table_name == custom_table

	def test_init_without_table_name(self):
		"""Test initialization when no table name is provided."""
		# Given: No environment variable or table name parameter
		if 'REPLICATION_PARAMETERS_TABLE' in os.environ:
			os.environ.pop('REPLICATION_PARAMETERS_TABLE')

		# When: We initialize a repository
		with patch('boto3.client'):
			repo = ParametersRepository()

		# Then: The table_name should be None and a warning should be logged
		assert repo.table_name is None

	def test_get_parameter(self, dynamodb_client, setup_dynamodb_parameters, dynamodb_tables):
		"""Test retrieving a parameter by name."""
		# Given: A repository and a parameter in DynamoDB
		repo = ParametersRepository(dynamodb_client=dynamodb_client, table_name=dynamodb_tables['parameters_table'])
		param_name = '/test-stack/test-source-bucket/test'

		# When: We get the parameter by name
		result = repo.get_parameter(param_name)

		# Then: We should get the correct parameter value
		assert result is not None
		assert 'destinations' in result
		assert len(result['destinations']) == 2
		assert result['destinations'][0]['region'] == 'us-west-2'
		assert result['destinations'][1]['bucket'] == 'target-bucket-eu'
		assert result['last_updated'] == 1619712000

	def test_get_parameter_nonexistent(self, dynamodb_client, setup_dynamodb_parameters, dynamodb_tables):
		"""Test retrieving a nonexistent parameter."""
		# Given: A repository
		repo = ParametersRepository(dynamodb_client=dynamodb_client, table_name=dynamodb_tables['parameters_table'])
		nonexistent_param = '/test-stack/nonexistent-bucket/test'

		# When: We try to get a nonexistent parameter
		result = repo.get_parameter(nonexistent_param)

		# Then: The result should be None
		assert result is None

	def test_get_parameter_error(self, dynamodb_client):
		"""Test handling errors when getting parameters."""
		# Given: A repository with an invalid table name
		repo = ParametersRepository(dynamodb_client=dynamodb_client, table_name='nonexistent-table')
		param_name = '/test-stack/test-source-bucket/test'

		# When: We try to get a parameter and an error occurs
		result = repo.get_parameter(param_name)

		# Then: The function should handle the error and return None
		assert result is None

	def test_get_parameter_with_prefix_exact_match(self, dynamodb_client, setup_dynamodb_parameters, dynamodb_tables):
		"""Test retrieving a parameter with an exact prefix match."""
		# Given: A repository and parameters in DynamoDB
		repo = ParametersRepository(dynamodb_client=dynamodb_client, table_name=dynamodb_tables['parameters_table'])
		stack_name = 'test-stack'
		bucket = 'test-source-bucket'
		monitored_prefix = 'test'

		# When: We get a parameter with a matching monitored prefix
		param_name, destinations = repo.get_parameter_with_prefix(stack_name, bucket, monitored_prefix)

		# Then: We should get the correct parameter name and destinations
		assert param_name == '/test-stack/test-source-bucket/test'
		assert len(destinations) == 2
		assert destinations[0]['region'] == 'us-west-2'
		assert destinations[1]['bucket'] == 'target-bucket-eu'

	def test_get_parameter_with_prefix_fallback_to_bucket(
		self, dynamodb_client, setup_dynamodb_parameters, dynamodb_tables
	):
		"""Test falling back to bucket-level parameter when prefix doesn't match."""
		# Given: A repository and parameters in DynamoDB
		repo = ParametersRepository(dynamodb_client=dynamodb_client, table_name=dynamodb_tables['parameters_table'])
		stack_name = 'test-stack'
		bucket = 'test-source-bucket'
		monitored_prefix = 'nonexistent-prefix'

		# When: We get a parameter with a non-matching monitored prefix
		param_name, destinations = repo.get_parameter_with_prefix(stack_name, bucket, monitored_prefix)

		# Then: We should fall back to the bucket-level parameter
		assert param_name == '/test-stack/test-source-bucket'
		assert len(destinations) == 1
		assert destinations[0]['region'] == 'us-west-1'
		assert destinations[0]['bucket'] == 'target-bucket-default'

	def test_get_parameter_with_prefix_no_match(self, dynamodb_client, setup_dynamodb_parameters, dynamodb_tables):
		"""Test when no parameter matches either the prefix or bucket."""
		# Given: A repository and parameters in DynamoDB
		repo = ParametersRepository(dynamodb_client=dynamodb_client, table_name=dynamodb_tables['parameters_table'])
		stack_name = 'test-stack'
		bucket = 'nonexistent-bucket'
		monitored_prefix = 'test'

		# When: We get a parameter with no matching prefix or bucket
		param_name, destinations = repo.get_parameter_with_prefix(stack_name, bucket, monitored_prefix)

		# Then: We should get empty results
		assert param_name == ''
		assert destinations is None

	def test_get_parameter_with_prefix_none_prefix(self, dynamodb_client, setup_dynamodb_parameters, dynamodb_tables):
		"""Test retrieving a parameter with None prefix should fall back to bucket level."""
		# Given: A repository and parameters in DynamoDB
		repo = ParametersRepository(dynamodb_client=dynamodb_client, table_name=dynamodb_tables['parameters_table'])
		stack_name = 'test-stack'
		bucket = 'test-source-bucket'
		monitored_prefix = None

		# When: We get a parameter with None as monitored prefix
		param_name, destinations = repo.get_parameter_with_prefix(stack_name, bucket, monitored_prefix)

		# Then: We should get the bucket-level parameter
		assert param_name == '/test-stack/test-source-bucket'
		assert len(destinations) == 1
		assert destinations[0]['bucket'] == 'target-bucket-default'

	def test_deserialize_item(self, dynamodb_client):
		"""Test deserializing a DynamoDB item."""
		# Given: A repository and a DynamoDB item
		repo = ParametersRepository(dynamodb_client=dynamodb_client)
		item = {
			'ParameterName': {'S': '/test-stack/test-bucket'},
			'Destinations': {
				'L': [
					{
						'M': {
							'region': {'S': 'us-west-2'},
							'bucket': {'S': 'target-bucket'},
							'kms_key_arn': {'S': 'arn:aws:kms:us-west-2:123456789012:key/test-key'},
							'storage_class': {'S': 'STANDARD'},
						}
					}
				]
			},
			'LastUpdated': {'N': '1619712000'},
		}

		# When: We deserialize the item
		result = repo._deserialize_item(item)

		# Then: We should get the correct deserialized dictionary
		assert result['parameter_name'] == '/test-stack/test-bucket'
		assert len(result['destinations']) == 1
		assert result['destinations'][0]['region'] == 'us-west-2'
		assert result['destinations'][0]['bucket'] == 'target-bucket'
		assert result['destinations'][0]['kms_key_arn'] == 'arn:aws:kms:us-west-2:123456789012:key/test-key'
		assert result['destinations'][0]['storage_class'] == 'STANDARD'
		assert result['last_updated'] == 1619712000

	def test_deserialize_destinations(self, dynamodb_client):
		"""Test deserializing destinations from a DynamoDB item."""
		# Given: A repository and a list of destinations in DynamoDB format
		repo = ParametersRepository(dynamodb_client=dynamodb_client)
		destinations_list = [
			{
				'M': {
					'region': {'S': 'us-west-2'},
					'bucket': {'S': 'target-bucket'},
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

		# When: We deserialize the destinations
		result = repo._deserialize_destinations(destinations_list)

		# Then: We should get the correct list of destination dictionaries
		assert len(result) == 2
		assert result[0]['region'] == 'us-west-2'
		assert result[0]['bucket'] == 'target-bucket'
		assert result[0]['kms_key_arn'] == 'arn:aws:kms:us-west-2:123456789012:key/test-key'
		assert result[0]['storage_class'] == 'STANDARD'
		assert result[1]['region'] == 'eu-west-1'
		assert result[1]['bucket'] == 'target-bucket-eu'
		assert result[1]['storage_class'] == 'STANDARD_IA'
		assert 'kms_key_arn' not in result[1]

	def test_deserialize_destinations_missing_fields(self, dynamodb_client):
		"""Test deserializing destinations with missing optional fields."""
		# Given: A repository and a list of destinations with missing fields
		repo = ParametersRepository(dynamodb_client=dynamodb_client)
		destinations_list = [
			{
				'M': {
					'region': {'S': 'us-west-2'},
					'bucket': {'S': 'target-bucket'},
					# Missing kms_key_arn and storage_class
				}
			}
		]

		# When: We deserialize the destinations
		result = repo._deserialize_destinations(destinations_list)

		# Then: We should get the correct deserialized dictionary with optional fields absent
		assert len(result) == 1
		assert result[0]['region'] == 'us-west-2'
		assert result[0]['bucket'] == 'target-bucket'
		assert 'kms_key_arn' not in result[0]
		assert 'storage_class' not in result[0]
