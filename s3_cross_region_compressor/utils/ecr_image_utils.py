"""
ECR image deployment utilities for the S3 Cross-Region Compressor.

This module provides utility functions to upload container images to S3 and
deploy them to ECR repositories. These functions are used to prepare the Docker images
for the source and target ECS tasks.
"""

from aws_cdk import (
	Fn,
	aws_iam as iam,
	aws_ecr as ecr,
	aws_s3_deployment as s3deploy,
	aws_s3 as s3,
	aws_kms as kms,
	aws_logs as logs,
)
from cdk_ecr_deployment import ECRDeployment, S3ArchiveName, DockerImageName


def s3_upload_assets(
	scope,
	s3_d_id: str,
	solution_repository: s3.Bucket,
	file_location: str,
	repository_kms_key: kms.Key,
) -> s3deploy.BucketDeployment:
	"""
	Upload assets to an S3 bucket.

	Uploads container images or other assets to an S3 bucket for
	later deployment to ECR.

	Args:
	    scope: The CDK construct scope
	    s3_d_id: Identifier for the S3 deployment
	    solution_repository: S3 bucket to upload to
	    file_location: Local path to the file to upload
	    repository_kms_key: KMS key for S3 bucket encryption/decryption

	Returns:
	    s3deploy.BucketDeployment: The S3 deployment construct
	"""
	s3_deployed = s3deploy.BucketDeployment(
		scope=scope,
		id=f's3-upload-assets-{s3_d_id}',
		sources=[s3deploy.Source.asset(file_location)],
		destination_bucket=solution_repository,
		log_retention=logs.RetentionDays.ONE_DAY,
		memory_limit=1024,
		extract=False,
		prune=False,
	)

	s3_deployed.handler_role.add_to_policy(
		iam.PolicyStatement(
			actions=[
				'kms:Encrypt',
				'kms:ReEncrypt*',
				'kms:GenerateDataKey*',
				'kms:DescribeKey',
				'kms:Decrypt',
			],
			effect=iam.Effect.ALLOW,
			resources=[repository_kms_key.key_arn],
		)
	)

	return s3_deployed


def ecr_deployment(
	scope,
	ecr_d_id: str,
	solution_repository: s3.Bucket,
	uploaded_object: s3deploy.BucketDeployment,
	ecr_repository: ecr.Repository,
	kms_key: kms.Key,
) -> ECRDeployment:
	"""
	Deploy a container image from S3 to ECR.

	Creates an ECR deployment that pulls a container image from S3
	and pushes it to an ECR repository.

	Args:
	    scope: The CDK construct scope
	    ecr_d_id: Identifier for the ECR deployment
	    solution_repository: S3 bucket containing the container image
	    uploaded_object: S3 bucket deployment that uploaded the image
	    ecr_repository: Target ECR repository for the image
	    kms_key: KMS key for encryption/decryption

	Returns:
	    ECRDeployment: The ECR deployment construct
	"""
	s3_image = f'{solution_repository.bucket_name}/{Fn.select(0, uploaded_object.object_keys)}'
	ecr_deployed = ECRDeployment(
		scope=scope,
		id=f'ecr-deployment-{ecr_d_id}',
		src=S3ArchiveName(s3_image),
		dest=DockerImageName(ecr_repository.repository_uri),
	)
	ecr_deployed.add_to_principal_policy(
		iam.PolicyStatement(
			actions=[
				'kms:Encrypt',
				'kms:ReEncrypt*',
				'kms:GenerateDataKey*',
				'kms:DescribeKey',
				'kms:Decrypt',
			],
			resources=[kms_key.key_arn],
			effect=iam.Effect.ALLOW,
		)
	)
	return ecr_deployed
