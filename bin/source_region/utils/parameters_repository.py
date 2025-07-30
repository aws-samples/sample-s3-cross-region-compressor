"""
Repository for replication parameters stored in DynamoDB.

This module provides a clean data access layer for storage and retrieval
of replication parameters from DynamoDB.
"""

import boto3
import logging
import os
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


class ParametersRepository:
	"""Repository for replication parameters stored in DynamoDB."""

	def __init__(self, dynamodb_client=None, table_name=None):
		"""
		Initialize the repository with DynamoDB client.

		Args:
		    dynamodb_client: Optional boto3 DynamoDB client
		    table_name: Name of the DynamoDB table for parameters
		"""
		self.dynamodb = dynamodb_client or boto3.client('dynamodb')
		self.table_name = table_name or os.environ.get('REPLICATION_PARAMETERS_TABLE')

		if not self.table_name:
			logger.warning(
				'REPLICATION_PARAMETERS_TABLE environment variable is not set. DynamoDB operations will fail.'
			)

	def get_parameter(self, parameter_name: str) -> Optional[Dict]:
		"""
		Retrieve parameter value by name.

		Args:
		    parameter_name: The full parameter name

		Returns:
		    Dictionary with parameter value or None if not found
		"""
		try:
			response = self.dynamodb.get_item(TableName=self.table_name, Key={'ParameterName': {'S': parameter_name}})

			if 'Item' in response:
				return self._deserialize_item(response['Item'])
			return None
		except Exception as e:
			logger.warning(f'Error getting parameter: {e}')
			return None

	def get_parameter_with_prefix(
		self, stack_name: str, bucket: str, monitored_prefix: str = None
	) -> Tuple[str, Optional[List[Dict]]]:
		"""
		Get parameter using the monitored prefix from environment variable.

		Args:
		    stack_name: Stack name for parameter path
		    bucket: Bucket name
		    monitored_prefix: Monitored prefix from environment variable (optional)

		Returns:
		    Tuple of (parameter_name, destinations)
		"""
		# If monitored_prefix is provided, use it directly
		if monitored_prefix:
			param_name = f'/{stack_name}/{bucket}/{monitored_prefix}'
			logger.debug(f'Using provided monitored prefix for parameter: {param_name}')

			param_value = self.get_parameter(param_name)
			if param_value and 'destinations' in param_value:
				logger.debug(f'Found matching parameter with monitored prefix: {param_name}')
				return param_name, param_value['destinations']

		# Fall back to bucket-level parameter
		param_name = f'/{stack_name}/{bucket}'
		logger.debug(f'Falling back to bucket-level parameter: {param_name}')

		param_value = self.get_parameter(param_name)
		if param_value and 'destinations' in param_value:
			logger.debug(f'Found matching parameter at bucket level: {param_name}')
			return param_name, param_value['destinations']

		# If we get here, no matching parameter was found
		logger.error(f'No parameter found for /{stack_name}/{bucket}/{monitored_prefix or ""}')
		return '', None

	def _deserialize_item(self, item: Dict) -> Dict:
		"""Convert DynamoDB format to Python dictionary."""
		result = {}
		if 'ParameterName' in item:
			result['parameter_name'] = item['ParameterName']['S']
		if 'Destinations' in item:
			result['destinations'] = self._deserialize_destinations(item['Destinations']['L'])
		if 'LastUpdated' in item:
			result['last_updated'] = int(item['LastUpdated']['N'])
		return result

	def _deserialize_destinations(self, destinations_list: List) -> List[Dict]:
		"""Convert DynamoDB format destinations to Python list of dictionaries."""
		result = []
		for dest in destinations_list:
			destination = {}
			dest_map = dest['M']

			if 'region' in dest_map:
				destination['region'] = dest_map['region']['S']
			if 'bucket' in dest_map:
				destination['bucket'] = dest_map['bucket']['S']
			if 'kms_key_arn' in dest_map and 'S' in dest_map['kms_key_arn']:
				destination['kms_key_arn'] = dest_map['kms_key_arn']['S']
			if 'storage_class' in dest_map and 'S' in dest_map['storage_class']:
				destination['storage_class'] = dest_map['storage_class']['S']
			if 'backup' in dest_map and 'BOOL' in dest_map['backup']:
				destination['backup'] = dest_map['backup']['BOOL']

			result.append(destination)
		return result
