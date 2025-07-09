"""
Baseline Resources Stack for S3 Cross-Region Compressor

This module defines the baseline infrastructure resources required for the S3 cross-region
compressor solution. It creates region-specific resources such as VPCs, S3 buckets,
SQS queues, KMS keys, and IAM roles based on whether a region is configured as a
source, target, or both.

The stack is designed to be deployed to multiple AWS regions, with different resources
created in each region based on its role in the cross-region compression solution.
"""

from typing import Any, Dict, List
from aws_cdk import (
	Tags,
	Stack,
)
from constructs import Construct

# Import resource modules
from s3_cross_region_compressor.resources.vpc import create_vpc, create_security_group
from s3_cross_region_compressor.resources.kms import create_key
from s3_cross_region_compressor.resources.s3_bucket import create_s3_bucket
from s3_cross_region_compressor.resources.ecs import create_ecs_fargate_cluster
from s3_cross_region_compressor.resources.notifications import create_alarm_topic

from s3_cross_region_compressor.source_baseline_stack import (
	SourceStackProps,
	SourceStack,
)
from s3_cross_region_compressor.target_baseline_stack import (
	TargetStackProps,
	TargetStack,
)


class BaselineRegionCompressorProps:
	"""
	Properties for the BaselineRegionCompressorStack.

	This class defines the properties required to create a baseline
	infrastructure stack in a specific AWS region.

	Attributes:
	    tags (Dict[str, Any]): Tags to apply to all resources in the stack
	    stack_name (str): Name of the stack for resource naming
	    vpc_cidr (str): CIDR block for the VPC
	    number_of_azs (int): Number of availability zones to use
	    source_target (str): Role of this region ('source', 'target', or 'both')
	    replication_config (Dict[str, Any]): Configuration for S3 bucket replication
	    min_capacity (int): Minimum number of ECS tasks
	    max_capacity (int): Maximum number of ECS tasks
	    scaling_target_backlog_per_task (int): Target number of messages per task
	    scale_out_cooldown (int): Cooldown period for scaling out in seconds
	    scale_in_cooldown (int): Cooldown period for scaling in in seconds
	    notification_emails (List[str]): Email addresses to notify for alarms
	"""

	def __init__(
		self,
		*,
		region_config: Dict[str, Any],
		tags: Dict[str, Any],
		replication_config: Dict[str, Any],
		stack_name: str,
		notification_emails: List[str],
		min_capacity: int = 0,
		max_capacity: int = 20,
		scaling_target_backlog_per_task: int = 30,
		scale_out_cooldown: int = 60,
		scale_in_cooldown: int = 90,
	):
		"""
		Initialize BaselineRegionCompressorProps.

		Args:
		    region_config (Dict[str, Any]): Configuration for the region including:
		        - vpc_cidr: CIDR block for the VPC
		        - availability_zones: Number of AZs to use
		        - source_target: Role of this region ('source', 'target', or 'both')
		    tags (Dict[str, Any]): Tags to apply to all resources
		    replication_config (Dict[str, Any]): Configuration for S3 bucket replication
		    stack_name (str): Name of the stack for resource naming
		    notification_emails (List[str]): Email addresses to notify for alarms
		    min_capacity (int): Minimum number of ECS tasks (default: 0)
		    max_capacity (int): Maximum number of ECS tasks (default: 20)
		    scaling_target_backlog_per_task (int): Target number of messages per task (default: 10)
		    scale_out_cooldown (int): Cooldown period for scaling out in seconds (default: 60)
		    scale_in_cooldown (int): Cooldown period for scaling in in seconds (default: 90)
		"""
		self.tags = tags
		self.stack_name = stack_name
		self.vpc_cidr = region_config['vpc_cidr']
		self.number_of_azs = region_config['availability_zones']
		self.source_target = region_config['source_target']
		self.replication_config = replication_config
		self.notification_emails = notification_emails
		self.min_capacity = min_capacity
		self.max_capacity = max_capacity
		self.scaling_target_backlog_per_task = scaling_target_backlog_per_task
		self.scale_out_cooldown = scale_out_cooldown
		self.scale_in_cooldown = scale_in_cooldown


class BaselineRegionCompressorStack(Stack):
	"""
	Creates baseline infrastructure resources in a specific AWS region.

	This stack creates the foundational resources required for the S3 cross-region
	compressor in each region, including:
	- VPC with private subnets
	- VPC endpoints for AWS services
	- S3 buckets for solution repositories (inbound/outbound)
	- SQS queues for S3 event notifications
	- KMS keys for encryption
	- IAM roles with appropriate permissions
	- SSM parameters for configuration

	The resources created depend on whether the region is configured as a source,
	target, or both. Source regions handle compression of objects and sending them
	to target regions, while target regions handle decompression and storage of
	objects received from source regions.
	"""

	def __init__(
		self,
		scope: Construct,
		construct_id: str,
		*,
		props: BaselineRegionCompressorProps,
		**kwargs: Any,
	) -> None:
		"""
		Initialize BaselineRegionCompressorStack.

		Creates the baseline infrastructure resources in a specific AWS region
		based on the provided properties.

		Args:
		    scope (Construct): CDK construct scope
		    construct_id (str): CDK construct ID
		    props (BaselineRegionCompressorProps): Properties for the stack
		    **kwargs (Any): Additional keyword arguments passed to the Stack constructor
		"""
		super().__init__(scope, construct_id, **kwargs)

		# Apply tags to all resources in the stack
		if props.tags:
			for key, value in props.tags.items():
				Tags.of(self).add(key=key, value=value)

		# Create VPC and Endpoints
		# This creates a VPC with private isolated subnets and necessary VPC endpoints
		self.vpc = create_vpc(scope=self, vpc_cidr=props.vpc_cidr, availability_zones=props.number_of_azs)
		self.repository_kms_key = create_key(scope=self, kms_id='repository')
		self.solution_repository = create_s3_bucket(
			scope=self,
			kms_key=self.repository_kms_key,
			s3_id='repo',
			stack_name=props.stack_name,
			expiration=1,
		)
		self.security_group = create_security_group(scope=self, vpc=self.vpc)
		self.ecs_cluster = create_ecs_fargate_cluster(scope=self, vpc=self.vpc)

		# Create SNS topic for alarms and subscribe notification emails
		self.alarm_topic = create_alarm_topic(
			scope=self, stack_name=props.stack_name, notification_emails=props.notification_emails, kms_key=self.repository_kms_key
		)

		if 'source' in props.source_target:
			SourceStack(
				scope=self,
				construct_id='SourceStack',
				props=SourceStackProps(
					replication_config=props.replication_config,
					stack_name=props.stack_name,
					security_group=self.security_group,
					ecs_cluster=self.ecs_cluster,
					repository_kms_key=self.repository_kms_key,
					solution_repository=self.solution_repository,
					min_capacity=props.min_capacity,
					max_capacity=props.max_capacity,
					scaling_target_backlog_per_task=props.scaling_target_backlog_per_task,
					scale_out_cooldown=props.scale_out_cooldown,
					scale_in_cooldown=props.scale_in_cooldown,
					alarm_topic=self.alarm_topic,
				),
			)

		if 'target' in props.source_target:
			self.target_stack = TargetStack(
				scope=self,
				construct_id='TargetStack',
				props=TargetStackProps(
					replication_config=props.replication_config,
					stack_name=props.stack_name,
					security_group=self.security_group,
					ecs_cluster=self.ecs_cluster,
					repository_kms_key=self.repository_kms_key,
					solution_repository=self.solution_repository,
					# min_capacity=props.min_capacity,
					# max_capacity=props.max_capacity,
					scaling_target_backlog_per_task=props.scaling_target_backlog_per_task,
					scale_out_cooldown=props.scale_out_cooldown,
					scale_in_cooldown=props.scale_in_cooldown,
					alarm_topic=self.alarm_topic,
				),
			)
