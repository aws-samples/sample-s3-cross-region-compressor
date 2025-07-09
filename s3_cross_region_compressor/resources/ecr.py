"""
ECR-related resource creation for the S3 Cross-Region Compressor.

This module provides functions for creating ECR repositories to store
container images for source and target regions.
"""

from aws_cdk import RemovalPolicy, aws_kms as kms, aws_ecr as ecr


def create_ecr_repository(scope, ecr_id: str, kms_key: kms.Key) -> ecr.Repository:
	"""
	Create an ECR repository for container images.

	Creates an ECR repository with appropriate lifecycle rules and
	KMS encryption for storing container images used in the solution.

	Args:
	    scope: The CDK construct scope
	    ecr_id: Identifier for the ECR repository
	    kms_key: KMS key for repository encryption

	Returns:
	    ecr.Repository: The created ECR repository
	"""

	return ecr.Repository(
		scope=scope,
		id=f'ecr-{ecr_id}-repository',
		lifecycle_rules=[ecr.LifecycleRule(max_image_count=10)],
		removal_policy=RemovalPolicy.DESTROY,
		empty_on_delete=True,
		encryption=ecr.RepositoryEncryption.KMS,
		encryption_key=kms_key,
	)
