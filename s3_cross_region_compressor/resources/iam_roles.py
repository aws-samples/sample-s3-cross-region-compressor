"""
Identity-related resource creation for the S3 Cross-Region Compressor.

This module provides functions for creating identity resources,
such as IAM roles and policies.
"""

from constructs import Construct
from aws_cdk import aws_iam as iam


def create_ecs_tasks_roles(scope: Construct, role_id: str, config_id: int = 0) -> iam.Role:
	"""
	Create an IAM role for ECS tasks.

	Creates an IAM role that can be assumed by ECS tasks, with
	permissions added separately based on the specific task requirements.

	Args:
		scope: The CDK construct scope
		role_id: Identifier for the role (e.g., 'source', 'target')
		config_id: Configuration identifier for multiple roles (default: 0)

	Returns:
		iam.Role: The created IAM role
	"""
	return iam.Role(
		scope,
		f'ecs-task-role-{role_id}-{config_id}',
		assumed_by=iam.ServicePrincipal('ecs-tasks.amazonaws.com'),
	)


def create_ecs_execution_roles(scope: Construct, role_id: str) -> iam.Role:
	"""
	Create an IAM role for ECS task execution.

	Creates an IAM role that can be assumed by the ECS task execution service,
	which is responsible for pulling container images and publishing logs.

	Args:
		scope: The CDK construct scope
		role_id: Identifier for the role (e.g., 'outbound', 'inbound')

	Returns:
		iam.Role: The created IAM role
	"""
	return iam.Role(
		scope,
		f'ecs-execution-role-{role_id}',
		assumed_by=iam.ServicePrincipal('ecs-tasks.amazonaws.com'),
	)


def create_s3_replication_role(scope: Construct, role_id: str, outbound_bucket_name: str):
	"""
	Create an IAM role for S3 cross-region replication.

	Creates an IAM role that can be assumed by the S3 service to perform
	cross-region replication, with appropriate permissions for reading from
	the source bucket and writing to destination buckets.

	Args:
		scope: The CDK construct scope
		role_id: Identifier for the role
		outbound_bucket_name: Name of the source bucket for replication

	Returns:
		iam.Role: The created IAM role with replication permissions
	"""
	role = iam.Role(
		scope,
		f's3-replication-role-{role_id}',
		assumed_by=iam.ServicePrincipal('s3.amazonaws.com'),
	)

	role.add_to_policy(
		iam.PolicyStatement(
			actions=['s3:GetReplicationConfiguration', 's3:ListBucket'],
			resources=[f'arn:aws:s3:::{outbound_bucket_name}'],
		)
	)

	role.add_to_policy(
		iam.PolicyStatement(
			actions=[
				's3:GetObjectVersionForReplication',
				's3:GetObjectVersionAcl',
				's3:GetObjectVersionTagging',
			],
			resources=[f'arn:aws:s3:::{outbound_bucket_name}/*'],
		)
	)
	role.add_to_policy(
		iam.PolicyStatement(
			actions=['kms:Decrypt'],
			resources=[f'arn:aws:kms:{scope.region}:{scope.account}:key/*'],
			conditions={'ForAnyValue:StringLike': {'kms:ResourceAliases': 'alias/outbound'}},
		)
	)

	return role
