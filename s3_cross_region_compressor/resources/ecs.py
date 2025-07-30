"""
ECS-related resource creation for the S3 Cross-Region Compressor.

This module provides functions for creating ECS resources,
such as Fargate clusters and related components for container workloads.
"""

from constructs import Construct
from aws_cdk import (
	RemovalPolicy,
	aws_ec2 as ec2,
	aws_ecs as ecs,
	aws_sqs as sqs,
	aws_ecr as ecr,
	aws_iam as iam,
	aws_s3 as s3,
)
from s3_cross_region_compressor.utils.ecs_utils import create_autoscaling_policy
from cdk_nag import NagSuppressions

def create_ecs_fargate_cluster(scope: Construct, vpc: ec2.Vpc) -> ecs.Cluster:
	"""
	Create an ECS Fargate cluster.

	Creates an ECS cluster in the specified VPC with Fargate capacity
	providers configured for cost optimization (using FARGATE_SPOT).

	Args:
		scope: The CDK construct scope
		vpc: VPC to create the cluster in

	Returns:
		ecs.Cluster: The created ECS cluster
	"""
	ecs_cluster = ecs.Cluster(
		scope,
		'ecs-fargate-cluster',
		vpc=vpc,
		enable_fargate_capacity_providers=True,
		container_insights_v2=ecs.ContainerInsights.ENHANCED,
	)
	ecs_cluster.apply_removal_policy(RemovalPolicy.DESTROY)
	ecs_cluster.add_default_capacity_provider_strategy(
		[ecs.CapacityProviderStrategy(capacity_provider='FARGATE_SPOT', weight=1)]
	)
	NagSuppressions.add_resource_suppressions(
    	ecs_cluster,
    	[
        	{
            	"id": "AwsSolutions-ECS4",
            	"reason": "Container Insights v2 (Enhanced) is enabled but CDK Nag only detects the legacy v1 'container_insights=True' parameter. We use the newer 'container_insights_v2=ecs.ContainerInsights.ENHANCED' which provides better monitoring capabilities."
        	}
    	]
	)
	return ecs_cluster


def create_task_definition(
	scope,
	task_d_id: str,
	stack_name: str,
	ecr_repository: ecr.Repository,
	ecs_task_role: iam.Role,
	ecs_execution_role: iam.Role,
	sqs_queue: sqs.Queue,
	s3_bucket: s3.Bucket,
	compression_settings_table_name: str = '',
	replication_parameters_table_name: str = '',
	cpu: str = '1024',
	memory: str = '2048',
	ephemeral_storage: int = None,
	data_transfer_cost: str = '0.03',
	fargate_cost_per_minute: str = '0.00072',
	monitored_prefix: str = None,
	backup_mode: bool = False,
	catalog_bucket_name: str = '',

) -> ecs.TaskDefinition:
	"""
	Create an ECS task definition for the S3 Cross-Region Compressor.

	Creates a Fargate-compatible task definition with the specified container image,
	IAM roles, and environment variables for the compression/decompression tasks.

	Args:
		scope: The CDK construct scope
		task_d_id: Identifier for the task definition
		ecr_repository: ECR repository containing the container image
		ecs_task_role: IAM role for the task
		ecs_execution_role: IAM role for task execution
		sqs_queue: SQS queue for receiving S3 event notifications
		s3_bucket: S3 bucket for input/output objects
		cpu: CPU units to allocate (default: '1024' = 1 vCPU)
		memory: Memory to allocate in MiB (default: '2048' = 2GB)
		ephemeral_storage: Ephemeral storage size in GiB (default: None, uses AWS default of 20 GiB)

	Returns:
		ecs.TaskDefinition: The created task definition
	"""

	variables = {
		'AWS_DEFAULT_REGION': scope.region,
		'BUCKET': s3_bucket.bucket_name,
		'SQS_QUEUE_URL': sqs_queue.queue_url,
		'STACK_NAME': stack_name,
		'COMPRESSION_SETTINGS_TABLE': compression_settings_table_name,
		'REPLICATION_PARAMETERS_TABLE': replication_parameters_table_name,
		'AWS_EMF_ENVIRONMENT': 'Local',
		'AWS_EMF_NAMESPACE': stack_name,
		'AWS_EMF_SERVICE_NAME': 'S3CrossRegionCompressor',
		'AWS_EMF_SERVICE_TYPE': 'AWS::ECS::Container',
		'AWS_EMF_LOG_GROUP_NAME': f'/aws/ecs/{stack_name}',
		'LOG_LEVEL': 'INFO',
		'DATA_TRANSFER_COST': data_transfer_cost,
		'FARGATE_COST_PER_MINUTE': fargate_cost_per_minute,
	}
	if monitored_prefix:
		variables['MONITORED_PREFIX'] = monitored_prefix
	if backup_mode:
		variables['BACKUP_MODE'] = 'true'
	if catalog_bucket_name:
		variables['CATALOG_BUCKET_NAME'] = catalog_bucket_name


	task_def_props = {
		'compatibility': ecs.Compatibility.FARGATE,
		'cpu': cpu,
		'memory_mib': memory,
		'network_mode': ecs.NetworkMode.AWS_VPC,
		'execution_role': ecs_execution_role,
		'task_role': ecs_task_role,
		'runtime_platform': ecs.RuntimePlatform(
			cpu_architecture=ecs.CpuArchitecture.ARM64,
			operating_system_family=ecs.OperatingSystemFamily.LINUX,
		),
	}

	# Add ephemeral storage if specified and greater than the default 20 GiB
	if ephemeral_storage and ephemeral_storage > 20:
		task_def_props['ephemeral_storage_gib'] = ephemeral_storage

	task_def = ecs.TaskDefinition(scope, f'task-definition-{task_d_id}', **task_def_props)
	task_def.add_container(
		id=task_d_id,
		image=ecs.ContainerImage.from_ecr_repository(ecr_repository),
		logging=ecs.LogDrivers.aws_logs(
			stream_prefix='ecs',
			mode=ecs.AwsLogDriverMode.NON_BLOCKING,
		),
		environment=variables,
		version_consistency=ecs.VersionConsistency.DISABLED,
	)
	task_def.apply_removal_policy(RemovalPolicy.DESTROY)

	# Suppress AwsSolutions-ECS2 as environment variables are required for application configuration
	NagSuppressions.add_resource_suppressions(
		task_def,
		[
			{
				"id": "AwsSolutions-ECS2",
				"reason": "Environment variables are required for application configuration and contain non-sensitive runtime parameters like bucket names, queue URLs, and service configuration"
			}
		]
	)

	return task_def


def create_ecs_service(
	scope,
	id: str,
	ecs_cluster: ecs.Cluster,
	task_definition: ecs.TaskDefinition,
	security_group: ec2.SecurityGroup,
	sqs_queue: sqs.Queue,
	min_capacity: int = 0,
	max_capacity: int = 20,
	scaling_target_backlog_per_task: int = 30,
	scale_out_cooldown: int = 60,
	scale_in_cooldown: int = 120,
) -> ecs.FargateService:
	"""
	Create an ECS Fargate service with SQS-based auto-scaling.

	Creates an ECS Fargate service with auto-scaling based on SQS queue depth,
	using FARGATE_SPOT for cost optimization.

	Args:
		id: Identifier for the service
		ecs_cluster: ECS cluster to create the service in
		task_definition: Task definition for the service
		security_group: Security group for the service
		sqs_queue: SQS queue to base scaling on
		min_capacity: Minimum number of tasks (default: 0)
		max_capacity: Maximum number of tasks (default: 20)
		scaling_target_backlog_per_task: Target number of messages per task (default: 10)
		scale_out_cooldown: Cooldown period for scaling out in seconds (default: 60)
		scale_in_cooldown: Cooldown period for scaling in in seconds (default: 300)

	Returns:
		ecs.FargateService: The created ECS service
	"""
	ecs_service = ecs.FargateService(
		scope=scope,
		id=f'ecs-fargate-{id}-service',
		service_name=id,
		cluster=ecs_cluster,
		task_definition=task_definition,
		assign_public_ip=False,
		security_groups=[security_group],
		capacity_provider_strategies=[ecs.CapacityProviderStrategy(capacity_provider='FARGATE_SPOT', weight=1)],
		min_healthy_percent=100,
		propagate_tags=ecs.PropagatedTagSource.SERVICE,
	)
	ecs_service.apply_removal_policy(RemovalPolicy.DESTROY)

	create_autoscaling_policy(
		scope=scope,
		id=id,
		ecs_cluster=ecs_cluster,
		ecs_service=ecs_service,
		sqs_queue=sqs_queue,
		min_capacity=min_capacity,
		max_capacity=max_capacity,
		scaling_target_backlog_per_task=scaling_target_backlog_per_task,
		scale_out_cooldown=scale_out_cooldown,
		scale_in_cooldown=scale_in_cooldown,
	)

	return ecs_service
