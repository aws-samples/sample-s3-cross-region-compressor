from typing import Any, Dict
from aws_cdk import (
	NestedStack,
	aws_ec2 as ec2,
	aws_iam as iam,
	aws_s3 as s3,
	aws_ecs as ecs,
	aws_kms as kms,
	aws_ecr as ecr,
	aws_dynamodb as ddb,
	aws_lambda as lambda_,
	aws_sns as sns,
)

from constructs import Construct

from s3_cross_region_compressor.resources.sqs import create_sqs_queue
from s3_cross_region_compressor.resources.alarms import (
	create_dlq_alarm,
	create_ecs_task_failures_alarm,
	create_max_capacity_alarm,
)
from s3_cross_region_compressor.resources.iam_roles import create_ecs_tasks_roles
from s3_cross_region_compressor.resources.ecs import (
	create_task_definition,
	create_ecs_service,
)
from s3_cross_region_compressor.utils.s3_utils import add_source_bucket_notification
from s3_cross_region_compressor.utils.iam_utils import (
	add_source_s3_read_permissions,
	add_cloudwatch_metrics_policy,
)
from s3_cross_region_compressor.resources.cost_estimator import cr_cost_estimator
from cdk_nag import NagSuppressions

class SourceServiceProps:
	"""
	Properties for the SourceServiceStack class.

	This class defines the properties required to create the source service infrastructure
	that handles the detection, compression, and preparation of objects for cross-region replication.

	Attributes:
		config_id (int): Unique identifier for this configuration
		replication_config (Dict[str, Any]): Configuration for the replication setup
		stack_name (str): Name of the stack for resource naming
		repository_kms_key (kms.Key): KMS key for the repository
		outbound_kms_key (kms.Key): KMS key for outbound data
		outbound_s3_bucket (s3.Bucket): S3 bucket for outbound data
		ecs_execution_role (iam.Role): IAM role for ECS execution
		ecr_repository (ecr.Repository): ECR repository for container images
		security_group (ec2.SecurityGroup): Security group for ECS tasks
		ecs_cluster (ecs.Cluster): ECS Fargate cluster
		min_capacity (int): Minimum number of ECS tasks
		max_capacity (int): Maximum number of ECS tasks
		scaling_target_backlog_per_task (int): Target number of messages per task
		scale_out_cooldown (int): Cooldown period for scaling out in seconds
		scale_in_cooldown (int): Cooldown period for scaling in in seconds
		alarm_topic (sns.Topic): SNS topic for alarms
	"""

	def __init__(
		self,
		*,
		config_id: int,
		replication_config: Dict[str, Any],
		stack_name: str,
		repository_kms_key: kms.Key,
		outbound_kms_key: kms.Key,
		outbound_s3_bucket: s3.Bucket,
		ecs_execution_role: iam.Role,
		ecr_repository: ecr.Repository,
		security_group: ec2.SecurityGroup,
		ecs_cluster: ecs.Cluster,
		min_capacity: int = 0,
		max_capacity: int = 20,
		scaling_target_backlog_per_task: int = 30,
		scale_out_cooldown: int = 60,
		scale_in_cooldown: int = 120,
		compression_settings_table: ddb.TableV2,
		replication_parameters_table: ddb.TableV2,
		cost_estimator_lambda: lambda_.Function,
		alarm_topic: sns.Topic,
	):
		"""
		Initialize source service properties.

		Args:
		    config_id: Configuration identifier
		    replication_config: Replication configuration
		    stack_name: Name of the stack
		    repository_kms_key: KMS key for the repository
		    outbound_kms_key: KMS key for outbound data
		    outbound_s3_bucket: S3 bucket for outbound data
		    ecs_execution_role: IAM role for ECS execution
		    ecr_repository: ECR repository
		    security_group: Security group
		    ecs_cluster: ECS cluster
		    min_capacity: Minimum number of tasks (default: 0)
		    max_capacity: Maximum number of tasks (default: 20)
		    scaling_target_backlog_per_task: Target number of messages per task (default: 10)
		    scale_out_cooldown: Cooldown period for scaling out in seconds (default: 60)
		    scale_in_cooldown: Cooldown period for scaling in in seconds (default: 300)
		    compression_settings_table: DynamoDB table for compression settings
		    replication_parameters_table: DynamoDB table for replication parameters
		    cost_estimator_lambda: Lambda function for cost estimation
		    alarm_topic: SNS topic for alarms
		"""
		self.stack_name = stack_name
		self.replication_config = replication_config
		self.config_id = config_id
		self.security_group = security_group
		self.ecs_cluster = ecs_cluster
		self.repository_kms_key = repository_kms_key
		self.outbound_kms_key = outbound_kms_key
		self.outbound_s3_bucket = outbound_s3_bucket
		self.ecs_execution_role = ecs_execution_role
		self.ecr_repository = ecr_repository
		self.min_capacity = min_capacity
		self.max_capacity = max_capacity
		self.scaling_target_backlog_per_task = scaling_target_backlog_per_task
		self.scale_out_cooldown = scale_out_cooldown
		self.scale_in_cooldown = scale_in_cooldown
		self.compression_settings_table = compression_settings_table
		self.replication_parameters_table = replication_parameters_table
		self.cost_estimator_lambda = cost_estimator_lambda
		self.alarm_topic = alarm_topic


class SourceServiceStack(NestedStack):
	"""
	Creates source service infrastructure for a specific replication configuration.

	This nested stack creates the resources needed for a specific source bucket replication
	configuration, including:
	- IAM roles for ECS tasks
	- SQS queue for S3 event notifications
	- S3 event notifications to detect new objects
	- DynamoDB tables for configuration storage
	- ECS task definition and service with auto-scaling

	The source service is responsible for detecting new objects in a source bucket,
	compressing them, and placing them in the outbound bucket for replication.
	"""

	def __init__(
		self,
		scope: Construct,
		construct_id: str,
		*,
		props: SourceServiceProps,
		**kwargs: Any,
	) -> None:
		"""
		Initialize SourceServiceStack.

		Creates the source service infrastructure for a specific replication
		configuration.

		Args:
		    scope (Construct): CDK construct scope
		    construct_id (str): CDK construct ID
		    props (SourceServiceProps): Properties for the stack
		    **kwargs (Any): Additional keyword arguments passed to the NestedStack constructor
		"""
		super().__init__(scope, construct_id, **kwargs)

		def sanitize_sqs_name(name):
			""" Sanitize the input string to be valid for SQS queue names.
			Replaces special characters like '/' with hyphens. """

			valid_chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_"
			result = ""
		
			for char in name:
				if char in valid_chars:
					result += char
				else:
					result += "-"  # Replace any invalid character with a hyphen

			return result

		# Sanitize the config_id for use in SQS queue name
		sanitized_config_id = sanitize_sqs_name(props.config_id)
		
		ecs_task_role = create_ecs_tasks_roles(scope=self, role_id='source', config_id=sanitized_config_id)

		## dlq_queue, sqs_queue = create_sqs_queue(
		##	scope=self,
		##	sqs_id=f'source-{props.config_id}',
		##	kms_key=props.outbound_kms_key,
		##	visibility_timeout=props.replication_config['source'].get('visibility_timeout', 300),
		##)

		dlq_queue, sqs_queue = create_sqs_queue(
			scope=self,
			sqs_id=f'source-{sanitized_config_id}',
			kms_key=props.outbound_kms_key,
			visibility_timeout=props.replication_config['source'].get('visibility_timeout', 300),
		)

		# Create DLQ alarm
		create_dlq_alarm(
			scope=self, id=f'dlq-source-{sanitized_config_id}', dlq_queue=dlq_queue, sns_topic=props.alarm_topic
		)

		# S3 notifications
		add_source_bucket_notification(
			scope=self,
			bucket_name=props.replication_config['source']['bucket'],
			sqs_queue=sqs_queue,
			prefix_filter=props.replication_config['source'].get('prefix_filter', ''),
			suffix_filter=props.replication_config['source'].get('suffix_filter', ''),
		)

		# Grant read access to the DynamoDB parameters table
		props.replication_parameters_table.grant_read_data(ecs_task_role)

		# Permissions
		props.outbound_s3_bucket.grant_write(ecs_task_role)
		props.outbound_kms_key.grant_encrypt_decrypt(ecs_task_role)
		sqs_queue.grant_consume_messages(ecs_task_role)
		add_source_s3_read_permissions(
			role=ecs_task_role,
			bucket_name=props.replication_config['source']['bucket'],
			kms_key_arn=props.replication_config['source'].get('kms_key_arn', ''),
		)
		props.compression_settings_table.grant_read_write_data(ecs_task_role)

		add_cloudwatch_metrics_policy(ecs_task_role)
		
		NagSuppressions.add_resource_suppressions(
			ecs_task_role,
			[
				{
					"id": "AwsSolutions-IAM5",
					"reason": "CloudWatch metrics require wildcard resources for namespace-based filtering",
					"applies_to": ["Resource::*"]
				}
			]
		)

		cpu = str(props.replication_config['source'].get('cpu', '2048'))
		memory = str(props.replication_config['source'].get('memory', '4096'))
		ephemeral_storage = props.replication_config['source'].get('ephemeral_storage', None)

		target_regions = []
		for destination in props.replication_config['destinations']:
			target_regions.append(destination['region'])

		cost_estimation = cr_cost_estimator(
			scope=self,
			id=f'source-{sanitized_config_id}',
			cost_estimator_lambda=props.cost_estimator_lambda,
			region=props.replication_config['source']['region'],
			fargate_cpu=cpu,
			fargate_memory=memory,
			target_regions=target_regions,
			fargate_ephemeral_disk=ephemeral_storage,
		)

		ecs_task_definition = create_task_definition(
			scope=self,
			task_d_id=f'source-{sanitized_config_id}',
			stack_name=props.stack_name,
			ecs_task_role=ecs_task_role,
			ecs_execution_role=props.ecs_execution_role,
			s3_bucket=props.outbound_s3_bucket,
			sqs_queue=sqs_queue,
			ecr_repository=props.ecr_repository,
			compression_settings_table_name=props.compression_settings_table.table_name,
			replication_parameters_table_name=props.replication_parameters_table.table_name,
			cpu=cpu,
			memory=memory,
			ephemeral_storage=ephemeral_storage,
			data_transfer_cost=cost_estimation.get_att_string('AverageDataTransferCostPerGB'),
			fargate_cost_per_minute=cost_estimation.get_att_string('FargateCostPerMinute'),
			monitored_prefix=props.replication_config['source'].get('prefix_filter', None),
		)

		max_capacity = props.replication_config['source'].get('scaling_limit', props.max_capacity)

		# ECS Service with autoscaling based on SQS queue depth
		ecs_service = create_ecs_service(
			scope=self,
			id=f'source-{sanitized_config_id}',
			ecs_cluster=props.ecs_cluster,
			task_definition=ecs_task_definition,
			security_group=props.security_group,
			sqs_queue=sqs_queue,
			min_capacity=props.replication_config['source'].get('min_capacity', props.min_capacity),
			max_capacity=max_capacity,
			scaling_target_backlog_per_task=props.replication_config['source'].get(
				'scaling_target_backlog_per_task', props.scaling_target_backlog_per_task
			),
			scale_out_cooldown=props.scale_out_cooldown,
			scale_in_cooldown=props.scale_in_cooldown,
		)

		# Create task failures alarm for each service
		create_ecs_task_failures_alarm(
			scope=self,
			id=f'source-{sanitized_config_id}',
			ecs_cluster=props.ecs_cluster,
			ecs_service=ecs_service,
			sns_topic=props.alarm_topic,
		)

		# Create max capacity alarm for each service
		create_max_capacity_alarm(
			scope=self,
			id=f'source-{sanitized_config_id}',
			ecs_cluster=props.ecs_cluster,
			ecs_service=ecs_service,
			max_capacity=max_capacity,
			sns_topic=props.alarm_topic,
		)
		NagSuppressions.add_stack_suppressions(
			self,
			[
				{
					"id": "AwsSolutions-IAM4",
					"reason": "Bucket notification handler is a CDK-generated resource that requires Lambda basic execution permissions",
					"applies_to": ["Resource::*BucketNotificationsHandler*Role*"]
				},
				{
					"id": "AwsSolutions-IAM5",
					"reason": "Bucket notification handler requires wildcard permissions to configure S3 notifications",
					"applies_to": ["Resource::*BucketNotificationsHandler*Role*DefaultPolicy*"]
				}
			]
		)
