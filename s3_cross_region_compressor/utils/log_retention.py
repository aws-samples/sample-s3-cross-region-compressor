"""
CloudWatch Log Group retention policy aspect for S3 Cross-Region Compressor

This module defines a CDK Aspect that applies a default retention policy to all
CloudWatch Log Groups in the application, preventing unlimited log retention.
"""

import jsii
from constructs import IConstruct
from aws_cdk import (
	IAspect,
	aws_logs as logs,
	aws_lambda as _lambda,
)


@jsii.implements(IAspect)
class LogGroupRetentionAspect:
	"""
	CDK Aspect that applies a retention policy to all CloudWatch Log Groups.

	This aspect will traverse the construct tree and apply the specified retention
	policy to all CloudWatch Log Groups that do not already have a retention policy.
	This ensures that all logs have a finite retention period, preventing unlimited
	log storage costs.
	"""

	def __init__(self, retention_days: int = 30):
		"""
		Initialize the LogGroupRetentionAspect.

		Args:
		    retention_days: The default retention period to apply (default: ONE_MONTH)
		"""
		self.retention_days = retention_days

	def visit(self, node: IConstruct) -> None:
		"""
		Visit a construct and apply the retention policy if it's a CloudWatch Log Group
		or a Lambda Function.

		This method is called for each construct in the CDK construct tree. It checks
		if the construct is a CloudWatch Log Group, and if so, applies the specified
		retention policy if one is not already defined.

		Args:
		    node: The construct to visit
		"""
		# Check if the node is a CloudWatch Log Group
		if isinstance(node, logs.CfnLogGroup):
			# Only apply retention if it's not already set
			if node.retention_in_days is None:
				node.retention_in_days = self.retention_days

		if isinstance(node, _lambda.Function):
			logs.LogRetention(
				node,
				f'{node.node.id}LogRetention',
				log_group_name=f'/aws/lambda/{node.function_name}',
				retention=logs.RetentionDays.ONE_DAY,
			)
