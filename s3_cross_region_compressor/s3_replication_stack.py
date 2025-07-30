from typing import Any, Dict
from aws_cdk import (
	Stack,
	custom_resources as cr,
	aws_iam as iam,
)
from constructs import Construct

from s3_cross_region_compressor.utils.s3_utils import add_replication_rule
from s3_cross_region_compressor.resources.iam_roles import create_s3_replication_role
from s3_cross_region_compressor.utils.iam_utils import add_target_s3_replication_permissions
from cdk_nag import NagSuppressions

class S3ReplicationProps:
	"""
	Properties for the S3ReplicationStack class.

	This class defines the properties required to configure S3 cross-region replication
	between source and target buckets.

	Attributes:
		replication_config (Dict): Configuration for S3 bucket replication
		stack_name (str): Name of the stack for resource naming
	"""

	def __init__(
		self,
		*,
		replication_config: Dict,
		stack_name: str,
	):
		"""
		Initialize S3ReplicationProps.

		Args:
		    replication_config (Dict): Configuration for replication between regions
		    stack_name (str): Name of the stack for resource naming
		"""
		self.stack_name = stack_name
		self.replication_config = replication_config


class S3ReplicationStack(Stack):
	"""
	Stack for setting up S3 cross-region replication.

	This stack creates the necessary resources for enabling S3 cross-region replication
	between source and target regions, including:
	- IAM role for S3 replication with appropriate permissions
	- Replication rules for each source-destination pair
	- Custom resource to apply the replication configuration to the S3 bucket

	The stack handles multiple replication configurations for each source region,
	allowing objects to be replicated to multiple target regions as needed.
	"""

	def __init__(
		self,
		scope: Construct,
		construct_id: str,
		*,
		props: S3ReplicationProps,
		**kwargs: Any,
	) -> None:
		"""
		Initialize S3ReplicationStack.

		Creates the S3 cross-region replication infrastructure based on the
		provided replication configuration.

		Args:
		    scope (Construct): CDK construct scope
		    construct_id (str): CDK construct ID
		    props (S3ReplicationProps): Properties for the stack
		    **kwargs (Any): Additional keyword arguments passed to the Stack constructor
		"""
		super().__init__(scope, construct_id, **kwargs)

		outbound_bucket = f'{props.stack_name}-{self.account}-{self.region}-outbound'

		s3_replication_role = create_s3_replication_role(
			scope=self, role_id='target', outbound_bucket_name=outbound_bucket
		)
		# For each outbound bucket, create a single replication configuration with multiple rules
		# Each rule corresponds to one destination bucket
		rules = []
		rule_priority = 1

		for item in props.replication_config:
			source_bucket = item['source_bucket']
			source_prefix = item['source_prefix']

			if source_prefix:
				prefix = f'{source_bucket}/{source_prefix}'
			else:
				prefix = source_bucket

			# Create a rule for each destination bucket
			for destination in item['destinations']:
				inbound_bucket = f'{props.stack_name}-{self.account}-{destination}-inbound'

				# Add permissions for the replication role to access the destination bucket
				add_target_s3_replication_permissions(
					role=s3_replication_role,
					target_bucket_name=inbound_bucket,
					target_region=destination,
					account_id=self.account,
				)

				# Create a unique rule for each source-destination pair with a unique ID and prefix
				rule = add_replication_rule(
					prefix=prefix,
					destination=inbound_bucket,
					target_region=destination,
					account_id=self.account,
					rule_priority=rule_priority,
				)

				rules.append(rule)
				rule_priority += 1

		# Create a single replication configuration with all rules
		if rules:
			cr.AwsCustomResource(
				scope=self,
				id='S3ReplicationPolicy',
				policy=cr.AwsCustomResourcePolicy.from_statements(
					[
						iam.PolicyStatement(
							effect=iam.Effect.ALLOW,
							actions=[
								's3:PutReplicationConfiguration',
								's3:GetReplicationConfiguration',
							],
							resources=[f'arn:aws:s3:::{outbound_bucket}'],
						),
						iam.PolicyStatement(
							effect=iam.Effect.ALLOW,
							actions=['iam:PassRole'],
							resources=[s3_replication_role.role_arn],
						),
					]
				),
				install_latest_aws_sdk=True,
				on_create=cr.AwsSdkCall(
					service='S3',
					action='putBucketReplication',
					parameters={
						'Bucket': outbound_bucket,
						'ReplicationConfiguration': {
							'Role': s3_replication_role.role_arn,
							'Rules': rules,
						},
					},
					physical_resource_id=cr.PhysicalResourceId.of('create-replication-policy-v2'),
				),
				on_update=cr.AwsSdkCall(
					service='S3',
					action='putBucketReplication',
					parameters={
						'Bucket': outbound_bucket,
						'ReplicationConfiguration': {
							'Role': s3_replication_role.role_arn,
							'Rules': rules,
						},
					},
					physical_resource_id=cr.PhysicalResourceId.of('update-replication-policy-v2'),
				),
			)
		NagSuppressions.add_resource_suppressions(
            s3_replication_role,
            [
                {
                    "id": "AwsSolutions-IAM5",
                    "reason": "S3 cross-region replication requires wildcard permissions: S3 object paths (/*) for replicating all objects with unpredictable names, and KMS key paths (key/*) with condition limiting access to keys with 'alias/inbound' alias only",
                    "applies_to": [
                        "Resource::arn:aws:s3:::*-outbound/*",
                        "Resource::arn:aws:s3:::*-inbound/*", 
                        "Resource::arn:aws:kms:*:*:key/*"
                    ]
                }
            ]
        )

        # Add suppressions for the Custom Resource and Lambda components
		NagSuppressions.add_stack_suppressions(
            self,
            [
                {
                    "id": "AwsSolutions-IAM4",
                    "reason": "CDK-generated custom resources require Lambda basic execution permissions",
                    "applies_to": ["Resource::AWS679*ServiceRole*"]
                },
                {
                    "id": "AwsSolutions-L1", 
                    "reason": "CDK-generated Lambda functions use predefined runtimes",
                    "applies_to": ["Resource::AWS679*"]
                },
                {
                    "id": "AwsSolutions-IAM5",
                    "reason": "CDK-generated resources require wildcards for proper functioning",
                    "applies_to": ["Resource::*LogRetention*ServiceRole*DefaultPolicy*"]
                }
            ]
        )
