"""
Repository for compression settings stored in DynamoDB.

This module provides a clean data access layer for storage and retrieval
of compression settings from DynamoDB with aggregated per-level metrics.
"""

import boto3
import logging
import os
import time
from typing import Dict, Optional

logger = logging.getLogger(__name__)


class CompressionSettingsRepository:
	"""Repository for compression settings stored in DynamoDB."""

	def __init__(self, dynamodb_client=None, table_name=None):
		"""
		Initialize the repository with DynamoDB client.

		Args:
		    dynamodb_client: Optional boto3 DynamoDB client
		    table_name: Name of the DynamoDB table for settings
		"""
		self.dynamodb = dynamodb_client or boto3.client('dynamodb')
		self.table_name = table_name or os.environ.get('COMPRESSION_SETTINGS_TABLE')

		if not self.table_name:
			logger.warning('COMPRESSION_SETTINGS_TABLE environment variable is not set. DynamoDB operations will fail.')

	def get_settings(self, key: str) -> Optional[Dict]:
		"""
		Retrieve settings for a specific key.

		Args:
		    key: The bucket/prefix key

		Returns:
		    Settings dictionary or None if not found
		"""
		try:
			response = self.dynamodb.get_item(TableName=self.table_name, Key={'BucketPrefix': {'S': key}})

			if 'Item' in response:
				return self._deserialize_item(response['Item'])
			return None
		except Exception as e:
			logger.warning(f'Error getting compression settings: {e}')
			return None

	def create_settings(self, key: str, default_level: int) -> bool:
		"""
		Create initial settings for a new key using the new schema.

		Args:
		    key: The bucket/prefix key
		    default_level: Default compression level (not stored in new schema)

		Returns:
		    True if successful, False otherwise
		"""
		try:
			self.dynamodb.put_item(
				TableName=self.table_name,
				Item={
					'BucketPrefix': {'S': key},
					'SumCpuFactor': {'N': '0'},
					'LevelStats': {'M': {}},
					'TotalProcessedFiles': {'N': '0'},
					'Version': {'N': '0'},
					'LastUpdated': {'N': str(int(time.time()))},
				},
				ConditionExpression='attribute_not_exists(BucketPrefix)',
			)
			return True
		except Exception as e:
			logger.debug(f'Error initializing settings (may already exist): {e}')
			return False

	def update_metrics(self, key: str, level: int, benefit: float, cpu_factor: float, file_count: int = 1) -> bool:
		"""
		Update metrics using atomic updates for aggregated level statistics.

		Args:
		    key: The bucket/prefix key
		    level: Compression level used
		    benefit: Cost-benefit score calculated
		    cpu_factor: CPU performance factor
		    file_count: Number of files processed

		Returns:
		    True if successful, False otherwise
		"""
		try:
			level_str = str(level)

			# Check if this level already exists in LevelStats
			response = self.dynamodb.get_item(
				TableName=self.table_name,
				Key={'BucketPrefix': {'S': key}},
				ProjectionExpression='LevelStats',
				ConsistentRead=True,
			)

			level_exists = False
			if 'Item' in response and 'LevelStats' in response['Item']:
				level_exists = level_str in response['Item']['LevelStats'].get('M', {})

			if level_exists:
				# Level exists, update its values atomically
				update_expr = """
                ADD Version :one, 
                    TotalProcessedFiles :file_count, 
                    SumCpuFactor :cpu_factor,
                    LevelStats.#level.trials :one,
                    LevelStats.#level.objects :file_count,
                    LevelStats.#level.sum_benefit :benefit
                SET LastUpdated = :time
                """
				expr_attr_names = {'#level': level_str}
				expr_attr_values = {
					':one': {'N': '1'},
					':file_count': {'N': str(file_count)},
					':cpu_factor': {'N': str(cpu_factor)},
					':benefit': {'N': str(benefit)},
					':time': {'N': str(int(time.time()))},
				}
			else:
				# Level doesn't exist, create it with initial values
				update_expr = """
                ADD Version :one, 
                    TotalProcessedFiles :file_count, 
                    SumCpuFactor :cpu_factor
                SET LevelStats.#level = :level_data,
                    LastUpdated = :time
                """
				expr_attr_names = {'#level': level_str}
				expr_attr_values = {
					':one': {'N': '1'},
					':file_count': {'N': str(file_count)},
					':cpu_factor': {'N': str(cpu_factor)},
					':level_data': {
						'M': {
							'trials': {'N': '1'},
							'objects': {'N': str(file_count)},
							'sum_benefit': {'N': str(benefit)},
						}
					},
					':time': {'N': str(int(time.time()))},
				}

			# Execute the update
			self.dynamodb.update_item(
				TableName=self.table_name,
				Key={'BucketPrefix': {'S': key}},
				UpdateExpression=update_expr,
				ExpressionAttributeNames=expr_attr_names,
				ExpressionAttributeValues=expr_attr_values,
			)
			return True
		except Exception as e:
			logger.error(f'Error updating metrics: {e}')
			return False

	def _deserialize_item(self, item: Dict) -> Dict:
		"""Convert DynamoDB format to Python dictionary for the new schema."""
		result = {
			'key': item['BucketPrefix']['S'],
			'sum_cpu_factor': float(item.get('SumCpuFactor', {'N': '0'})['N']),
			'total_processed_files': int(item.get('TotalProcessedFiles', {'N': '0'})['N']),
			'version': int(item.get('Version', {'N': '0'})['N']),
			'last_updated': int(item.get('LastUpdated', {'N': str(int(time.time()))})['N']),
		}

		# Convert LevelStats from DynamoDB format
		level_stats = {}
		if 'LevelStats' in item and 'M' in item['LevelStats']:
			for level_str, data in item['LevelStats']['M'].items():
				# Extract objects field if it exists, otherwise default to same as trials
				objects = data['M'].get('objects', {'N': data['M']['trials']['N']})

				level_stats[int(level_str)] = {
					'sum_benefit': float(data['M']['sum_benefit']['N']),
					'trials': int(data['M']['trials']['N']),
					'objects': int(objects['N']),
				}

		result['level_stats'] = level_stats
		return result
