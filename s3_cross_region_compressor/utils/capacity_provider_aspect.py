"""
Capacity Provider Aspect for ECS Clusters

This module provides a CDK Aspect that fixes dependency issues with ECS capacity providers
during stack destruction. It ensures proper dependency relationships between
ECS services, clusters, and capacity provider associations to enable clean stack deletion.
"""

import jsii
from aws_cdk import IAspect
from constructs import IConstruct
import aws_cdk.aws_ecs as ecs


@jsii.implements(IAspect)
class HotfixCapacityProviderDependencies:
	"""
	Fixes dependency issues with ECS capacity providers during stack destruction.

	This aspect adds proper dependencies between:
	1. The capacity provider association and the cluster
	2. Each service and the capacity provider association

	This ensures resources are destroyed in the proper order, preventing the
	"capacity provider is in use and cannot be removed" error.
	See: https://github.com/aws/aws-cdk/issues/19275
	"""

	def visit(self, node: IConstruct) -> None:
		"""
		Visit each construct in the CDK app tree and fix capacity provider dependencies.

		Args:
		    node (IConstruct): The current construct being visited
		"""
		# Check if node is an ECS service (either EC2 or Fargate)
		if isinstance(node, ecs.Ec2Service) or isinstance(node, ecs.FargateService):
			# Find all children of the cluster
			children = node.cluster.node.find_all()
			for child in children:
				# If we find a capacity provider association...
				if isinstance(child, ecs.CfnClusterCapacityProviderAssociations):
					# Add a dependency from capacity provider association to the cluster
					child.node.add_dependency(node.cluster)
					# Add a dependency from the service to the capacity provider association
					node.node.add_dependency(child)
