"""
VPC-related resource creation for the S3 Cross-Region Compressor.

This module provides functions for creating VPC resources,
such as VPCs with private subnets and VPC endpoints.
"""

from constructs import Construct
from aws_cdk import aws_ec2 as ec2
from cdk_nag import NagSuppressions

def create_vpc_endpoints(scope: Construct, vpc: ec2.Vpc) -> None:
	"""
	Create VPC endpoints for AWS services.

	Creates gateway endpoints for S3 and DynamoDB, and interface endpoints
	for other AWS services based on whether the region is a source or not.

	Args:
	    scope: The CDK construct scope
	    vpc: The VPC to create endpoints in
	"""
	vpc.add_gateway_endpoint(
		's3-endpoint',
		service=ec2.GatewayVpcEndpointAwsService.S3,
	)
	vpc.add_gateway_endpoint(
		'dynamodb-endpoint',
		service=ec2.GatewayVpcEndpointAwsService.DYNAMODB,
	)

	list_of_endpoints = [
		ec2.InterfaceVpcEndpointAwsService.ECR,
		ec2.InterfaceVpcEndpointAwsService.ECR_DOCKER,
		ec2.InterfaceVpcEndpointAwsService.CLOUDWATCH_LOGS,
		ec2.InterfaceVpcEndpointAwsService.SQS,
	]

	for endpoint_type in list_of_endpoints:
		resource_id = endpoint_type.short_name + '-endpoint'
		endpoint = vpc.add_interface_endpoint(        #added endpoint as variable 
			id=resource_id,
			service=endpoint_type,
			subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PRIVATE_ISOLATED),
			)
		if endpoint.connections and endpoint.connections.security_groups:
			for sg in endpoint.connections.security_groups:
				NagSuppressions.add_resource_suppressions(
                    sg,
                    [
                        {
                            'id': 'AwsSolutions-EC23',
                            'reason': 'Security group for VPC endpoint with dynamic references'
                        }
                    ]
                )


def create_vpc(scope: Construct, vpc_cidr: str, availability_zones: int) -> ec2.Vpc:
	"""
	Create a VPC with private isolated subnets.

	Creates a VPC with the specified CIDR block and number of availability zones,
	with private isolated subnets for security.
	Also creates VPC Endpoints needed for the solution.

	Args:
	    scope: The CDK construct scope
	    vpc_cidr: CIDR block for the VPC
	    availability_zones: Number of availability zones to use

	Returns:
	    ec2.Vpc: The created VPC
	"""
	vpc = ec2.Vpc(
		scope,
		'vpc',
		ip_addresses=ec2.IpAddresses.cidr(vpc_cidr),
		max_azs=availability_zones,
		subnet_configuration=[
			ec2.SubnetConfiguration(
				name='subnet',
				subnet_type=ec2.SubnetType.PRIVATE_ISOLATED,
				cidr_mask=24,
			)
		],
	)
	# Suppress the VPC Flow Log rule
	NagSuppressions.add_resource_suppressions(
        vpc,
        [
            {
                'id': 'AwsSolutions-VPC7',
                'reason': 'Flow logs not required for this VPC as per project requirements'
            }
        ]
    )

	create_vpc_endpoints(scope, vpc)

	return vpc


def create_security_group(scope: Construct, vpc: ec2.Vpc) -> ec2.SecurityGroup:
	"""
	Create a Security Group for ECS Services.

	Creates a Security Group in the specified VPC that allows all outbound
	traffic, intended to be used by ECS Services in the solution.

	Args:
	    scope: The CDK construct scope
	    vpc: The VPC to create the security group in

	Returns:
	    ec2.SecurityGroup: The created security group
	"""
	return ec2.SecurityGroup(scope, 'security-group', vpc=vpc, allow_all_outbound=True)
