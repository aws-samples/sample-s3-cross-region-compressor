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
from cdk_nag import NagSuppressions

from s3_cross_region_compressor.resources.kms import create_key
from s3_cross_region_compressor.utils.dynamodb_utils import seed_parameters_table
from s3_cross_region_compressor.resources.s3_bucket import create_s3_bucket
from s3_cross_region_compressor.resources.ecr import create_ecr_repository
from s3_cross_region_compressor.resources.dynamodb import (
	create_compression_settings_ddb_table,
	create_parameters_ddb_table,
)
from s3_cross_region_compressor.utils.ecr_image_utils import (
	s3_upload_assets,
	ecr_deployment,
)
from s3_cross_region_compressor.resources.iam_roles import create_ecs_execution_roles

from s3_cross_region_compressor.source_service_stack import (
	SourceServiceProps,
	SourceServiceStack,
)

from s3_cross_region_compressor.resources.cost_estimator import create_lambda
from s3_cross_region_compressor.resources.dashboard import create_compression_dashboard
from s3_cross_region_compressor.resources.lambda_functions import create_migration_lambda
from s3_cross_region_compressor.resources.step_functions import create_migration_state_machine


class SourceStackProps:
	"""
	Properties for the SourceStack class.

	This class defines the properties required to create the source region
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
		max_capacity: int = 20,
		scaling_target_backlog_per_task: int = 30,
		scale_out_cooldown: int = 60,
		scale_in_cooldown: int = 120,
	):
		"""
		Initialize source stack properties.

		Args:
		    replication_config: Configuration for replication
		    stack_name: Name of the stack
		    security_group: Security group
		    ecs_cluster: ECS cluster
		    repository_kms_key: KMS key for the repository
		    solution_repository: S3 bucket for the solution repository
		    alarm_topic: SNS topic for alarms
		    min_capacity: Minimum number of tasks (default: 0)
		    max_capacity: Maximum number of tasks (default: 20)
		    scaling_target_backlog_per_task: Target number of messages per task (default: 10)
		    scale_out_cooldown: Cooldown period for scaling out in seconds (default: 60)
		    scale_in_cooldown: Cooldown period for scaling in in seconds (default: 300)
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
		self.scaling_target_backlog_per_task = scaling_target_backlog_per_task
		self.scale_out_cooldown = scale_out_cooldown
		self.scale_in_cooldown = scale_in_cooldown


class SourceStack(NestedStack):
	"""
	Creates source region infrastructure resources.

	This nested stack creates the infrastructure resources required for the source region
	in the S3 cross-region compressor solution, including:
	- KMS key for outbound data encryption
	- S3 bucket for outbound objects
	- ECR repository for the source region container image
	- IAM roles for ECS execution
	- Source service stacks for each replication configuration

	The source region is responsible for detecting new objects in source S3 buckets,
	compressing them, and placing them in the outbound bucket for replication.
	"""

	def __init__(
		self,
		scope: Construct,
		construct_id: str,
		*,
		props: SourceStackProps,
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

		self.outbound_kms_key = create_key(scope=self, kms_id='outbound')
		self.outbound_s3_bucket = create_s3_bucket(
			scope=self,
			kms_key=self.outbound_kms_key.add_alias('outbound'),
			s3_id='outbound',
			stack_name=props.stack_name,
		)

		# Create DynamoDB table for adaptive compression settings
		self.compression_settings_table = create_compression_settings_ddb_table(self)

		# Create DynamoDB table for replication parameters
		self.replication_parameters_table = create_parameters_ddb_table(self)

		# Seed the parameters table with data from replication_config.json
		seed_parameters_table(self, self.replication_parameters_table, props.replication_config, props.stack_name)

		# We'll create DLQ alarms in the service stacks where we have direct references to the SQS DLQs
		self.ecr_repository = create_ecr_repository(scope=self, ecr_id='outbound', kms_key=props.repository_kms_key)
		uploaded_s3_object = s3_upload_assets(
			scope=self,
			s3_d_id='outbound',
			solution_repository=props.solution_repository,
			file_location='./bin/dist/source_region.tar',
			repository_kms_key=props.repository_kms_key,
		)
		self.ecr_deployment = ecr_deployment(
			scope=self,
			ecr_d_id='outbound',
			solution_repository=props.solution_repository,
			uploaded_object=uploaded_s3_object,
			ecr_repository=self.ecr_repository,
			kms_key=props.repository_kms_key,
		)
		self.ecs_execution_role = create_ecs_execution_roles(scope=self, role_id='outbound')
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

		cost_estimator_lambda = create_lambda(self)

		migration_lambda = create_migration_lambda(self, props.stack_name, self.replication_parameters_table.table_name)
		migration_sfn = create_migration_state_machine(self, migration_lambda)

		# Create CloudWatch Dashboard to visualize compression metrics
		self.compression_dashboard = create_compression_dashboard(scope=self, stack_name=props.stack_name)

		for config_id, config in enumerate(props.replication_config):
			if config['source']['region'] == self.region:
				prefix_filter = config["source"].get("prefix_filter", "")
				if prefix_filter:
					source = f'{config["source"]["bucket"]}-{prefix_filter}'
				else:
					source = config["source"]["bucket"]

				SourceServiceStack(
					scope=self,
					construct_id=f'SourceService-{source}',
					props=SourceServiceProps(
						config_id=source,
						replication_config=config,
						stack_name=props.stack_name,
						repository_kms_key=props.repository_kms_key,
						outbound_kms_key=self.outbound_kms_key,
						outbound_s3_bucket=self.outbound_s3_bucket,
						ecs_execution_role=self.ecs_execution_role,
						ecr_repository=self.ecr_repository,
						ecs_cluster=props.ecs_cluster,
						security_group=props.security_group,
						min_capacity=props.min_capacity,
						max_capacity=props.max_capacity,
						scaling_target_backlog_per_task=props.scaling_target_backlog_per_task,
						scale_out_cooldown=props.scale_out_cooldown,
						scale_in_cooldown=props.scale_in_cooldown,
						compression_settings_table=self.compression_settings_table,
						replication_parameters_table=self.replication_parameters_table,
						cost_estimator_lambda=cost_estimator_lambda,
						alarm_topic=props.alarm_topic,
					),
				)
		#Suppression for Lambda Basic permissions
		NagSuppressions.add_stack_suppressions(
            self,
            [
                {
                    "id": "AwsSolutions-IAM4",
                    "reason": "CDK-generated custom resources require Lambda basic execution permissions",
                    "applies_to": ["Resource::AWS679.*ServiceRole.*"] 
                },
        		{
            		"id": "AwsSolutions-L1",
            		"reason": "This is a CDK-generated Lambda function for custom resources which we cannot directly control the runtime for",
            		"applies_to": ["Resource::AWS679.*"]
        		},
        		# New suppressions for IAM5 - wildcard permissions
        		{
         			"id": "AwsSolutions-IAM5",
            		"reason": "CDK-generated deployment resources require these permissions to deploy assets",
            		"applies_to": [
						"Resource::*CDKBucketDeployment*ServiceRole*",
						"Resource::*CDKECRDeployment*ServiceRole*",
						"Resource::*LogRetention*ServiceRole*",
						"Resource::*BucketNotificationsHandler*Role*"
            			]
        		},
				# For the ECS task definition environment variables warning
				{
					"id": "AwsSolutions-ECS2",
					"reason": "Task definitions use environment variables for configuration which are not sensitive",
					"applies_to": ["Resource::*task-definition*"]
				}
        	]
		)
