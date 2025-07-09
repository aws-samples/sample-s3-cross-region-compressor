"""
Storage-related resource creation for the S3 Cross-Region Compressor.

This module provides functions for creating storage resources,
such as S3 buckets with appropriate security settings.
"""

from constructs import Construct
from aws_cdk import Duration, RemovalPolicy, aws_s3 as s3, aws_kms as kms
from cdk_nag import NagSuppressions


def create_s3_bucket(scope: Construct, kms_key: kms.Key, s3_id: str, stack_name: str, expiration: int = 1) -> s3.Bucket:
	"""
	Create a solution repository S3 bucket.

	Creates an S3 bucket with appropriate security settings, encryption,
	and lifecycle rules for storing solution artifacts.

	Args:
	    scope: The CDK construct scope
	    kms_key: The KMS key to use for bucket encryption
	    s3_id: Identifier for the S3 bucket
	    stack_name: Name of the stack for bucket naming
	    expiration: Days before noncurrent versions expire (default: 1)

	Returns:
	    s3.Bucket: The created and configured bucket
	"""

	#return s3.Bucket(
	bucket = s3.Bucket(	
		scope=scope,
		id=f'solution-repository-{s3_id}',
		block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
		bucket_name=f'{stack_name}-{scope.account}-{scope.region}-{s3_id}',
		enforce_ssl=True,
		versioned=True,
		removal_policy=RemovalPolicy.DESTROY,
		auto_delete_objects=True,
		encryption=s3.BucketEncryption.KMS,
		encryption_key=kms_key,
		bucket_key_enabled=True,
		notifications_skip_destination_validation=True,
		lifecycle_rules=[
			s3.LifecycleRule(
				enabled=True,
				expiration=Duration.days(expiration),
				noncurrent_version_expiration=Duration.days(expiration),
				abort_incomplete_multipart_upload_after=Duration.days(expiration),
			)
		],
	)
	NagSuppressions.add_resource_suppressions(
        bucket,
        [
            {
                'id': 'AwsSolutions-S1',
                'reason': 'Access logs are not required for this bucket as per project requirements.'
            }
        ]
    )

	return bucket
