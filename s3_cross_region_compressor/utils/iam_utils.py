"""
Utility functions for IAM roles and policies.

This module provides helper functions for working with IAM roles and policies,
such as creating ECS task roles and adding common policy statements.
"""

from typing import Optional, Tuple
from constructs import Construct
from aws_cdk import aws_iam as iam


def create_ecs_task_roles(scope: Construct, source_target: str) -> Tuple[Optional[iam.Role], Optional[iam.Role]]:
	"""
	Create IAM roles for ECS tasks.

	Creates IAM roles for source and/or target ECS tasks based on the
	region configuration. The roles are assumed by the ECS task service
	principal.

	Args:
	    scope: The CDK construct scope
	    source_target: Role of this region ('source', 'target', or 'both')

	Returns:
	    Tuple containing:
	    - Source IAM role (or None if not a source region)
	    - Target IAM role (or None if not a target region)
	"""
	source_role = None
	target_role = None

	if source_target in ('both', 'source'):
		source_role = iam.Role(
			scope,
			'ecs-source-role',
			assumed_by=iam.ServicePrincipal('ecs-tasks.amazonaws.com'),
		)
	if source_target in ('both', 'target'):
		target_role = iam.Role(
			scope,
			'ecs-target-role',
			assumed_by=iam.ServicePrincipal('ecs-tasks.amazonaws.com'),
		)

	return source_role, target_role


def add_cloudwatch_metrics_policy(role: iam.Role, namespace: str = 'S3Compressor') -> None:
	"""
	Add CloudWatch metrics permissions to an IAM role.

	Adds a policy statement to the role that allows putting metric data
	to CloudWatch with the specified namespace.

	Args:
	    role: The IAM role to add permissions to
	    namespace: The CloudWatch namespace to restrict permissions to
	"""
	role.add_to_policy(
		iam.PolicyStatement(
			actions=['cloudwatch:PutMetricData'],
			resources=['*'],
			conditions={'StringEquals': {'cloudwatch:namespace': namespace}},
		)
	)


def add_source_s3_read_permissions(role: iam.Role, bucket_name: str, kms_key_arn: str) -> None:
	"""
	Add S3 read permissions to an IAM role.

	Adds policy statements to the role that allow reading objects from
	the specified S3 bucket.

	Args:
	    role: The IAM role to add permissions to
	    bucket_name: The name of the S3 bucket
	"""
	role.add_to_policy(
		iam.PolicyStatement(
			actions=[
				's3:GetObject',
				's3:GetObjectVersion',
				's3:GetObjectAttributes',
				's3:GetObjectTagging',
				's3:ListBucket',
			],
			resources=[f'arn:aws:s3:::{bucket_name}/*'],
		)
	)
	role.add_to_policy(
		iam.PolicyStatement(
			actions=['s3:ListAllMyBuckets', 's3:ListBucket'],
			resources=[f'arn:aws:s3:::{bucket_name}'],
		)
	)

	if kms_key_arn:
		role.add_to_policy(
			iam.PolicyStatement(
				actions=[
					'kms:Decrypt',
				],
				resources=[
					kms_key_arn,
				],
			)
		)


def add_s3_write_permissions(role: iam.Role, bucket_name: str) -> None:
	"""
	Add S3 write permissions to an IAM role.

	Adds a policy statement to the role that allows writing objects to
	the specified S3 bucket.

	Args:
	    role: The IAM role to add permissions to
	    bucket_name: The name of the S3 bucket
	"""
	role.add_to_policy(
		iam.PolicyStatement(
			actions=[
				's3:PutObject',
				's3:PutObjectTagging',
				's3:PutObjectAcl',
			],
			resources=[f'arn:aws:s3:::{bucket_name}/*'],
		)
	)


def add_kms_encrypt_permissions(role: iam.Role, kms_key_arn: str) -> None:
	"""
	Add KMS encrypt permissions to an IAM role.

	Adds a policy statement to the role that allows encrypting data
	with the specified KMS key.

	Args:
	    role: The IAM role to add permissions to
	    kms_key_arn: The ARN of the KMS key
	"""
	role.add_to_policy(
		iam.PolicyStatement(
			actions=[
				'kms:Encrypt',
				'kms:ReEncryptFrom',
				'kms:ReEncryptTo',
				'kms:GenerateDataKey',
				'kms:GenerateDataKeyWithoutPlaintext',
				'kms:DescribeKey',
				'kms:Decrypt',
			],
			resources=[
				kms_key_arn,
			],
		)
	)


def add_target_s3_write_permissions(role: iam.Role, bucket_name: str, kms_key_arn: str):
	"""
	Add S3 write permissions for target buckets to an IAM role.

	Adds policy statements to the role that allow writing objects to
	the specified target S3 bucket, including KMS encryption permissions
	if a KMS key ARN is provided.

	Args:
	    role: The IAM role to add permissions to
	    bucket_name: The name of the target S3 bucket
	    kms_key_arn: The ARN of the KMS key for encryption (optional)
	"""
	role.add_to_policy(
		iam.PolicyStatement(
			actions=[
				's3:PutObject',
				's3:PutObjectTagging',
				's3:PutObjectAcl',
			],
			resources=[f'arn:aws:s3:::{bucket_name}/*'],
		)
	)
	if kms_key_arn:
		role.add_to_policy(
			iam.PolicyStatement(
				actions=[
					'kms:Encrypt',
					'kms:ReEncryptFrom',
					'kms:ReEncryptTo',
					'kms:GenerateDataKey',
					'kms:GenerateDataKeyWithoutPlaintext',
					'kms:DescribeKey',
					'kms:Decrypt',
				],
				resources=[kms_key_arn],
			)
		)


def add_target_s3_replication_permissions(role: iam.Role, target_bucket_name: str, target_region: str, account_id: str):
	"""
	Add permissions required for S3 cross-region replication to a role.

	Args:
	    role: The IAM role to add permissions to
	    target_bucket_name: Name of the destination bucket
	    target_region: AWS region of the destination bucket
	    account_id: AWS account ID
	"""
	# Permission to replicate objects to the target bucket
	role.add_to_policy(
		iam.PolicyStatement(
			actions=['s3:ReplicateObject', 's3:ReplicateDelete', 's3:ReplicateTags'],
			resources=[f'arn:aws:s3:::{target_bucket_name}/*'],
		)
	)

	# Permission to verify bucket existence and get bucket location
	role.add_to_policy(
		iam.PolicyStatement(
			actions=['s3:GetBucketVersioning'],
			resources=[f'arn:aws:s3:::{target_bucket_name}'],
		)
	)

	# KMS key permissions if using encryption
	role.add_to_policy(
		iam.PolicyStatement(
			actions=['kms:Encrypt', 'kms:Decrypt'],
			resources=[f'arn:aws:kms:{target_region}:{account_id}:key/*'],
			conditions={'ForAnyValue:StringLike': {'kms:ResourceAliases': 'alias/inbound'}},
		)
	)
