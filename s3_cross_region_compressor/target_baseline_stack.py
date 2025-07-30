from typing import Any, Dict
from aws_cdk import (
	NestedStack,
	aws_ec2 as ec2,
	aws_iam as iam,
	aws_s3 as s3,
	aws_ecs as ecs,
	aws_kms as kms,
	aws_sns as sns,
)
from constructs import Construct

from s3_cross_region_compressor.resources.kms import create_key
from s3_cross_region_compressor.resources.s3_bucket import create_s3_bucket
from s3_cross_region_compressor.utils.s3_utils import add_inbound_bucket_notification
from s3_cross_region_compressor.resources.ecr import create_ecr_repository
from s3_cross_region_compressor.resources.alarms import (
	create_dlq_alarm,
	create_ecs_task_failures_alarm,
	create_max_capacity_alarm,
)
from s3_cross_region_compressor.resources.ecs import (
	create_task_definition,
	create_ecs_service,
)
from s3_cross_region_compressor.resources.sqs import create_sqs_queue
from s3_cross_region_compressor.utils.ecr_image_utils import (
	s3_upload_assets,
	ecr_deployment,
)
from s3_cross_region_compressor.resources.iam_roles import (
	create_ecs_execution_roles,
	create_ecs_tasks_roles,
)
from s3_cross_region_compressor.utils.iam_utils import (
	add_target_s3_write_permissions,
	add_cloudwatch_metrics_policy,
)
from s3_cross_region_compressor.resources.catalog_bucket import create_catalog_bucket
from s3_cross_region_compressor.resources.glue_catalog import (
	create_glue_database,
	create_glue_crawler_role,
	create_glue_crawler,
	create_crawler_schedule
)
from s3_cross_region_compressor.resources.athena_workgroup import (
	create_athena_query_results_bucket,
	create_athena_workgroup,
	create_athena_user_role
)

from cdk_nag import NagSuppressions

class TargetStackProps:
	"""
	Properties for the TargetStack class.

	This class defines the properties required to create target region
	infrastructure resources.

	Attributes:
		replication_config (Dict[str, Any]): Configuration for S3 bucket replication
		stack_name (str): Name of the stack for resource naming
		security_group (ec2.SecurityGroup): Security group for ECS tasks
		ecs_cluster (ecs.Cluster): ECS Fargate cluster
		repository_kms_key (kms.Key): KMS key for the repository
		solution_repository (s3.Bucket): S3 bucket for the solution repository
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
		replication_config: Dict[str, Any],
		stack_name: str,
		security_group: ec2.SecurityGroup,
		ecs_cluster: ecs.Cluster,
		repository_kms_key: kms.Key,
		solution_repository: s3.Bucket,
		alarm_topic: sns.Topic,
		min_capacity: int = 0,
		max_capacity: int = 60,
		scaling_target_backlog_per_task: int = 60,
		scale_out_cooldown: int = 60,
		scale_in_cooldown: int = 90,
	):
		"""
		Initialize target stack properties.

		Args:
		    replication_config: Configuration for replication
		    stack_name: Name of the stack
		    security_group: Security group for ECS tasks
		    ecs_cluster: ECS Fargate cluster
		    repository_kms_key: KMS key for the repository
		    solution_repository: S3 bucket for the solution repository
		    alarm_topic: SNS topic for alarms
		    min_capacity: Minimum number of tasks (default: 0)
		    max_capacity: Maximum number of tasks (default: 20)
		    scaling_target_backlog_per_task: Target number of messages per task (default: 30)
		    scale_out_cooldown: Cooldown period for scaling out in seconds (default: 60)
		    scale_in_cooldown: Cooldown period for scaling in in seconds (default: 120)
		"""
		self.stack_name = stack_name
		self.replication_config = replication_config
		self.security_group = security_group
		self.ecs_cluster = ecs_cluster
		self.repository_kms_key = repository_kms_key
		self.solution_repository = solution_repository
		self.alarm_topic = alarm_topic
		self.min_capacity = min_capacity
		self.max_capacity = max_capacity
		self.scaling_target_backlog_per_task = max(
			scaling_target_backlog_per_task / 10, 10
		)  # Target scales more aggressively as it's not batching SQS Messages.
		self.scale_out_cooldown = scale_out_cooldown
		self.scale_in_cooldown = scale_in_cooldown


class TargetStack(NestedStack):
	"""
	Creates target region infrastructure resources.

	This nested stack creates the infrastructure resources required for the target region
	in the S3 cross-region compressor solution, including:
	- KMS key for inbound data encryption
	- S3 bucket for inbound objects
	- ECR repository for the target region container image
	- IAM roles for ECS execution
	- S3 event notifications
	- ECS task definition and service with auto-scaling

	The target region is responsible for detecting new objects in the inbound bucket,
	decompressing them, and placing them in the target destination buckets.
	"""

	def __init__(
		self,
		scope: Construct,
		construct_id: str,
		*,
		props: TargetStackProps,
		**kwargs: Any,
	) -> None:
		"""
		Initialize TargetStack.

		Creates the target infrastructure resources in a specific AWS region
		based on the provided properties.

		Args:
		    scope (Construct): CDK construct scope
		    construct_id (str): CDK construct ID
		    props (TargetStackProps): Properties for the stack
		    **kwargs (Any): Additional keyword arguments passed to the NestedStack constructor
		"""
		super().__init__(scope, construct_id, **kwargs)

		self.inbound_kms_key = create_key(scope=self, kms_id='inbound')
		self.inbound_s3_bucket = create_s3_bucket(
			scope=self,
			kms_key=self.inbound_kms_key.add_alias('inbound'),
			s3_id='inbound',
			stack_name=props.stack_name,
		)
		
		# Create catalog bucket only if this region has backup destinations
		has_backup_destinations = any(
			dest.get('backup', False) for config in props.replication_config 
			for dest in config['destinations'] if dest['region'] == self.region
		)
		
		self.catalog_bucket = None
		self.glue_database = None
		self.glue_crawler = None
		self.athena_workgroup = None
		
		if has_backup_destinations:
			self.catalog_bucket = create_catalog_bucket(
				scope=self,
				kms_key=self.inbound_kms_key,
				stack_name=props.stack_name
			)
			
			# Create Glue database and crawler
			database_name = f'{props.stack_name}_catalog_db'
			self.glue_database = create_glue_database(
				scope=self,
				database_name=database_name
			)
			
			crawler_role = create_glue_crawler_role(scope=self)
			self.glue_crawler = create_glue_crawler(
				scope=self,
				database=self.glue_database,
				catalog_bucket=self.catalog_bucket,
				crawler_role=crawler_role,
				stack_name=props.stack_name
			)
			
			# Schedule crawler to run daily
			create_crawler_schedule(scope=self, crawler=self.glue_crawler)
			
			# Create Athena workgroup and query results bucket
			query_results_bucket = create_athena_query_results_bucket(
				scope=self,
				kms_key=self.inbound_kms_key,
				stack_name=props.stack_name
			)
			
			self.athena_workgroup = create_athena_workgroup(
				scope=self,
				query_results_bucket=query_results_bucket,
				stack_name=props.stack_name
			)
			
			# Create Athena user role
			self.athena_user_role = create_athena_user_role(
				scope=self,
				catalog_bucket=self.catalog_bucket,
				query_results_bucket=query_results_bucket,
				database_name=database_name
			)
			
			# Add suppressions for Athena query results bucket
			NagSuppressions.add_resource_suppressions(
				query_results_bucket,
				[
					{
						"id": "AwsSolutions-S1",
						"reason": "Query results bucket is temporary storage, access logs not required"
					}
				]
			)
			
			# Add suppressions for Glue crawler
			NagSuppressions.add_resource_suppressions(
				self.glue_crawler,
				[
					{
						"id": "AwsSolutions-GL1",
						"reason": "CloudWatch log encryption causes circular dependencies in CDK. Catalog data is encrypted in S3 with KMS, and crawler logs contain only operational metadata."
					}
				]
			)
		

		self.ecr_repository = create_ecr_repository(scope=self, ecr_id='inbound', kms_key=props.repository_kms_key)
		uploaded_s3_object = s3_upload_assets(
			scope=self,
			s3_d_id='inbound',
			solution_repository=props.solution_repository,
			file_location='./bin/dist/target_region.tar',
			repository_kms_key=props.repository_kms_key,
		)
		self.ecr_deployment = ecr_deployment(
			scope=self,
			ecr_d_id='inbound',
			solution_repository=props.solution_repository,
			uploaded_object=uploaded_s3_object,
			ecr_repository=self.ecr_repository,
			kms_key=props.repository_kms_key,
		)
		self.ecs_execution_role = create_ecs_execution_roles(scope=self, role_id='inbound')
		props.repository_kms_key.grant_decrypt(self.ecs_execution_role)
		self.ecr_repository.grant_pull(self.ecs_execution_role)
		self.ecs_execution_role.add_managed_policy(
			iam.ManagedPolicy.from_aws_managed_policy_name('service-role/AmazonECSTaskExecutionRolePolicy')
		)
		self.ecs_execution_role.add_to_policy(
			iam.PolicyStatement(
				actions=['ecs:TagResource', 'ecs:UntagResource'],
				resources=['*'],
			)
		)

		ecs_task_role = create_ecs_tasks_roles(
			scope=self,
			role_id='target',
		)

		dlq_queue, sqs_queue = create_sqs_queue(scope=self, sqs_id='target-sqs-queue', kms_key=self.inbound_kms_key)

		# S3 notifications
		add_inbound_bucket_notification(
			scope=self,
			bucket_name=self.inbound_s3_bucket.bucket_name,
			sqs_queue=sqs_queue,
		)

		create_dlq_alarm(scope=self, id='target-dlq', dlq_queue=dlq_queue, sns_topic=props.alarm_topic)

		# Handle circular dependency issue with KMS key policy
		# This allows the S3 bucket to use the KMS key for encryption
		# See: https://github.com/aws/aws-cdk/issues/3067
		self.inbound_kms_key.node.default_child.add_property_override(
			'KeyPolicy.Statement.1.Condition',
			{
				'ArnLike': {
					'aws:SourceArn': f'arn:aws:s3:::{props.stack_name}-{self.account}-{self.region}-inbound',
				}
			},
		)

		# Permissions
		self.inbound_s3_bucket.grant_read_write(ecs_task_role)
		if self.catalog_bucket:
			self.catalog_bucket.grant_read_write(ecs_task_role)
		self.inbound_kms_key.grant_encrypt_decrypt(ecs_task_role)
		sqs_queue.grant_consume_messages(ecs_task_role)

		add_cloudwatch_metrics_policy(ecs_task_role)

		for config in props.replication_config:
			for destination in config['destinations']:
				if destination['region'] == self.region:
					add_target_s3_write_permissions(
						role=ecs_task_role,
						bucket_name=destination['bucket'],
						kms_key_arn=destination.get('kms_key_arn', ''),
					)

		# Extract monitored prefix from replication config for target region
		monitored_prefix = None
		for config in props.replication_config:
			for destination in config['destinations']:
				if destination['region'] == self.region:
					# Get the prefix_filter from the source config that targets this region
					source_config = config.get('source', {})
					monitored_prefix = source_config.get('prefix_filter', '')
					break
			if monitored_prefix is not None:
				break
		
		ecs_task_definition = create_task_definition(
			scope=self,
			task_d_id='target-task-definition',
			stack_name=props.stack_name,
			ecs_task_role=ecs_task_role,
			ecs_execution_role=self.ecs_execution_role,
			s3_bucket=self.inbound_s3_bucket,
			sqs_queue=sqs_queue,
			ecr_repository=self.ecr_repository,
			monitored_prefix=monitored_prefix,
			backup_mode=has_backup_destinations,
			catalog_bucket_name=self.catalog_bucket.bucket_name if self.catalog_bucket else '',
		)

		# ECS Service with autoscaling based on SQS queue depth
		ecs_service = create_ecs_service(
			scope=self,
			id='target-service',
			ecs_cluster=props.ecs_cluster,
			task_definition=ecs_task_definition,
			security_group=props.security_group,
			sqs_queue=sqs_queue,
			min_capacity=props.min_capacity,
			max_capacity=props.max_capacity,
			scaling_target_backlog_per_task=props.scaling_target_backlog_per_task,
			scale_out_cooldown=props.scale_out_cooldown,
			scale_in_cooldown=props.scale_in_cooldown,
		)

		# Create task failures alarm
		create_ecs_task_failures_alarm(
			scope=self,
			id='target-service',
			ecs_cluster=props.ecs_cluster,
			ecs_service=ecs_service,
			sns_topic=props.alarm_topic,
		)

		# Create max capacity alarm
		create_max_capacity_alarm(
			scope=self,
			id='target-service',
			ecs_cluster=props.ecs_cluster,
			ecs_service=ecs_service,
			max_capacity=props.max_capacity,
			sns_topic=props.alarm_topic,
		)
		NagSuppressions.add_stack_suppressions(
            self,
            [
                # Managed policies
                {
                    "id": "AwsSolutions-IAM4",
                    "reason": "ECS execution roles require managed policies for basic functionality; CDK custom resources use Lambda execution role",
                    "applies_to": [
                        "Resource::*ExecutionRole*",
                        "Resource::*CDK*ServiceRole*",
                        "Resource::AWS679*",
                        "Resource::*LogRetention*",
                        "Resource::*BucketNotificationsHandler*"
                    ]
                },
                # Wildcard permissions
                {
                    "id": "AwsSolutions-IAM5",
                    "reason": "S3/KMS operations require wildcards for efficient object handling and encryption",
                    "applies_to": [
                        "Resource::*CDK*ServiceRole*DefaultPolicy*",
                        "Resource::*LogRetention*ServiceRole*DefaultPolicy*",
                        "Resource::*BucketNotificationsHandler*Role*DefaultPolicy*",
                        "Resource::*ecs-execution-role*DefaultPolicy*",
                        "Resource::*ecs-task-role*DefaultPolicy*"
                    ]
                },
                # Lambda runtime version
                {
                    "id": "AwsSolutions-L1",
                    "reason": "CDK-generated Lambda functions for custom resources use predefined runtimes",
                    "applies_to": [
                        "Resource::*CDK*",
                        "Resource::AWS679*"
                    ]
                }
            ]
        )
        # Add specific suppressions for the ECS execution role
		NagSuppressions.add_resource_suppressions(
            self.ecs_execution_role,
            [
                {
                    "id": "AwsSolutions-IAM4",
                    "reason": "ECS execution role requires AmazonECSTaskExecutionRolePolicy managed policy"
                },
                {
                    "id": "AwsSolutions-IAM5",
                    "reason": "ECS execution role needs wildcards for container execution and logging"
                }
            ]
        )
        # Add specific suppressions for the ECS task role
		NagSuppressions.add_resource_suppressions(
            ecs_task_role,
            [
                {
                    "id": "AwsSolutions-IAM5",
                    "reason": "S3 operations require wildcards for efficient object handling and bucket operations"
                }
            ]
        )
        
		# Add suppressions for catalog bucket if it exists
		if self.catalog_bucket:
			NagSuppressions.add_resource_suppressions(
				self.catalog_bucket,
				[
					{
						"id": "AwsSolutions-S1",
						"reason": "Catalog bucket is for metadata storage only, access logs not required"
					}
				]
			)
			
			# Add suppressions for Athena user role
			NagSuppressions.add_resource_suppressions(
				self.athena_user_role,
				[
					{
						"id": "AwsSolutions-IAM5",
						"reason": "Athena requires wildcards for Glue catalog and query operations"
					}
				]
			)
