"""
DynamoDB utilities for S3 cross-region compressor.

This module provides utility functions for working with DynamoDB tables, including:
- Seeding parameters tables with replication configuration
- Converting destination objects to DynamoDB format
"""

import time
from aws_cdk import custom_resources as cr, aws_iam as iam
from cdk_nag import NagSuppressions

def seed_parameters_table(scope, replication_parameters_table, replication_config, stack_name):
	"""
	Seed the parameters table with data from the replication configuration.

	Uses a custom resource to populate the DynamoDB table with parameter values.

	Args:
	    scope: The CDK construct scope
	    replication_parameters_table: The DynamoDB table to seed
	    replication_config: The replication configuration from which to extract parameters
	    stack_name: Stack name for parameter path prefixes
	"""
	# Create a Lambda function to seed the DynamoDB table
	seeder_role = iam.Role(
		scope,
		'DynamoDBSeederRole',
		assumed_by=iam.ServicePrincipal('lambda.amazonaws.com'),
		managed_policies=[iam.ManagedPolicy.from_aws_managed_policy_name('service-role/AWSLambdaBasicExecutionRole')],
	)

	# Suppress the specific warning for this resource
	NagSuppressions.add_resource_suppressions(
    	seeder_role,
    	[
        	{
            	"id": "AwsSolutions-IAM4",
            	"reason": "The AWSLambdaBasicExecutionRole is the minimum required for Lambda CloudWatch logging and is appropriate for this use case."
        	}
    	]
	)

	# Grant permissions to write to the DynamoDB table
	replication_parameters_table.grant_write_data(seeder_role)

	# Create entries for each replication configuration
	for config in replication_config:
		source = config['source']
		destinations = config['destinations']

		# Create the parameter key
		bucket = source['bucket']
		prefix = source.get('prefix_filter', '')
		param_name = f'/{stack_name}/{bucket}'
		if prefix:
			param_name = f'{param_name}/{prefix}'

		# Create a Lambda-backed custom resource for DynamoDB seeding
		seeder_handler = cr.AwsCustomResource(
			scope,
			f'ParameterSeeder-{bucket}-{prefix}'.replace('/', '-'),
			on_create=cr.AwsSdkCall(
				service='dynamodb',
				action='putItem',
				parameters={
					'TableName': replication_parameters_table.table_name,
					'Item': {
						'ParameterName': {'S': param_name},
						'Destinations': {'L': destinations_to_dynamodb_format(destinations)},
						'LastUpdated': {'N': str(int(time.time()))},
					},
				},
				physical_resource_id=cr.PhysicalResourceId.of(f'{param_name}-seeder'),
			),
			on_update=cr.AwsSdkCall(
				service='dynamodb',
				action='putItem',
				parameters={
					'TableName': replication_parameters_table.table_name,
					'Item': {
						'ParameterName': {'S': param_name},
						'Destinations': {'L': destinations_to_dynamodb_format(destinations)},
						'LastUpdated': {'N': str(int(time.time()))},
					},
				},
				physical_resource_id=cr.PhysicalResourceId.of(f'{param_name}-seeder'),
			),
			policy=cr.AwsCustomResourcePolicy.from_statements(
				[iam.PolicyStatement(actions=['dynamodb:PutItem'], resources=[replication_parameters_table.table_arn])]
			),
		)


def destinations_to_dynamodb_format(destinations):
	"""
	Convert destination objects to DynamoDB format.

	Args:
	    destinations: List of destination dictionaries

	Returns:
	    List of destinations in DynamoDB format
	"""
	result = []

	for dest in destinations:
		dest_item = {'M': {}}

		# Region is required
		dest_item['M']['region'] = {'S': dest['region']}

		# Bucket is required
		dest_item['M']['bucket'] = {'S': dest['bucket']}

		# Optional fields
		if 'kms_key_arn' in dest:
			dest_item['M']['kms_key_arn'] = {'S': dest['kms_key_arn']}

		if 'storage_class' in dest:
			dest_item['M']['storage_class'] = {'S': dest['storage_class']}

		# Add backup flag
		if 'backup' in dest:
			dest_item['M']['backup'] = {'BOOL': dest['backup']}

		result.append(dest_item)

	return result
