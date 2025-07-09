"""
CDK construct for cost estimation for S3 Cross-Region Compressor.

This module provides a CDK construct for creating a Custom Resource
that estimates the cost of running ECS Fargate tasks and data transfer
between AWS regions.
"""

from constructs import Construct
from aws_cdk import aws_lambda as lambda_, aws_iam as iam, Duration, CustomResource, CfnOutput, Stack
from typing import List


def cr_cost_estimator(
	scope: Construct,
	id: str,
	cost_estimator_lambda: lambda_.Function,
	region: str,
	fargate_cpu: int,
	fargate_memory: int,
	target_regions: List[str],
	fargate_ephemeral_disk: int = 20,
):
	# Create the Custom Resource
	cost_estimator_cr = CustomResource(
		scope,
		f'CostEstimatorCustomResource-{id}',
		service_token=cost_estimator_lambda.function_arn,
		properties={
			'AwsRegion': region,
			'FargateCpu': fargate_cpu,
			'FargateMemory': fargate_memory,
			'FargateEphemeralDisk': fargate_ephemeral_disk,
			'TargetRegions': target_regions,
		},
	)

	# Create outputs
	scope.fargate_cost_per_minute = cost_estimator_cr.get_att_string('FargateCostPerMinute')
	scope.avg_data_transfer_cost = cost_estimator_cr.get_att_string('AverageDataTransferCostPerGB')

	# Create CloudFormation outputs
	Stack.of(scope)
	CfnOutput(
		scope,
		'FargateCostPerMinuteOutput',
		value=scope.fargate_cost_per_minute,
		description=f'Fargate ARM SPOT cost per minute for {fargate_cpu} CPU units and {fargate_memory}GB memory',
	)

	CfnOutput(
		scope,
		'AvgDataTransferCostOutput',
		value=scope.avg_data_transfer_cost,
		description=f'Average data transfer cost per GB from {region} to {str(target_regions)}',
	)

	return cost_estimator_cr


def create_lambda(self) -> lambda_.Function:
	"""
	Create the Lambda function for cost estimation.

	Returns:
	    lambda_.Function: The created Lambda function
	"""
	# Create the Lambda function
	cost_estimator_lambda = lambda_.Function(
		self,
		'CostEstimatorFunction',
		runtime=lambda_.Runtime.PYTHON_3_13,
		architecture=lambda_.Architecture.ARM_64,
		handler='cost_estimator_cr.lambda_handler',
		code=lambda_.Code.from_asset('s3_cross_region_compressor/cr/'),
		timeout=Duration.seconds(30),
		memory_size=256,
		description='Lambda function for estimating AWS costs for S3 Cross-Region Compressor',
	)

	# Add permissions to query AWS Pricing API
	cost_estimator_lambda.add_to_role_policy(
		iam.PolicyStatement(actions=['pricing:GetProducts', 'pricing:DescribeServices'], resources=['*'])
	)

	return cost_estimator_lambda
