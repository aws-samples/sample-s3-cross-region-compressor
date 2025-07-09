"""
DynamoDB tables for the application.

This module provides the CloudFormation resources for creating DynamoDB tables:
- Compression settings table for storing historical metrics for adaptive compression
- Replication parameters table for storing target region information (replacing SSM parameters)
"""

from aws_cdk import aws_dynamodb as ddb, RemovalPolicy


def create_compression_settings_ddb_table(scope):
	# Create the DynamoDB table for compression settings
	return ddb.TableV2(
		scope=scope,
		id='compression-settings',
		partition_key=ddb.Attribute(name='BucketPrefix', type=ddb.AttributeType.STRING),
		billing=ddb.Billing.on_demand(),
		removal_policy=RemovalPolicy.DESTROY,
		point_in_time_recovery_specification=ddb.PointInTimeRecoverySpecification(point_in_time_recovery_enabled=True),
	)


def create_parameters_ddb_table(scope):
	"""
	Create a DynamoDB table for storing replication parameters.
	This table replaces the SSM Parameters previously used for target region information.

	Args:
		scope: The CDK construct scope

	Returns:
		ddb.TableV2: The created DynamoDB table
	"""
	return ddb.TableV2(
		scope=scope,
		id='replication-parameters',
		partition_key=ddb.Attribute(name='ParameterName', type=ddb.AttributeType.STRING),
		billing=ddb.Billing.on_demand(),
		removal_policy=RemovalPolicy.DESTROY,
		point_in_time_recovery_specification=ddb.PointInTimeRecoverySpecification(point_in_time_recovery_enabled=True),
	)
