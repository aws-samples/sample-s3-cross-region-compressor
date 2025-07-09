"""
Security-related resource creation for the S3 Cross-Region Compressor.

This module provides functions for creating security-related resources,
such as KMS keys for encryption.
"""

from constructs import Construct
from aws_cdk import RemovalPolicy, aws_kms as kms


def create_key(scope: Construct, kms_id: str) -> kms.Key:
	"""
	Create a KMS key for encryption.

	Creates a KMS key with key rotation enabled and a removal policy
	of DESTROY for development environments.

	Args:
	    scope: The CDK construct scope
	    kms_id: Identifier for the KMS key

	Returns:
	    kms.Key: The created KMS key
	"""
	return kms.Key(
		scope,
		f'kms-{kms_id}',
		enable_key_rotation=True,
		removal_policy=RemovalPolicy.DESTROY,
	)
