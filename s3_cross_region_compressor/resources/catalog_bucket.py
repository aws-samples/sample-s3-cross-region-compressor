"""
S3 bucket for storing backup file catalog metadata.
"""

from aws_cdk import (
    RemovalPolicy,
    Duration,
    aws_s3 as s3,
    aws_kms as kms,
)
from constructs import Construct


def create_catalog_bucket(scope: Construct, kms_key: kms.Key, stack_name: str) -> s3.Bucket:
    """
    Create S3 bucket for backup catalog metadata storage.
    
    Args:
        scope: CDK construct scope
        kms_key: KMS key for bucket encryption
        stack_name: Stack name for bucket naming
        
    Returns:
        s3.Bucket: The created catalog bucket
    """
    bucket = s3.Bucket(
        scope,
        'catalog-bucket',
        bucket_name=f'{stack_name}-{scope.account}-{scope.region}-catalog',
        encryption=s3.BucketEncryption.KMS,
        encryption_key=kms_key,
        versioned=False,
        removal_policy=RemovalPolicy.DESTROY,
        auto_delete_objects=True,
        enforce_ssl=True,  # Require SSL
        server_access_logs_bucket=None,  # Disable access logs for catalog bucket
    )
    
    return bucket