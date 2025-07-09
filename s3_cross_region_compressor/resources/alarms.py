"""
Alarm-related resource creation for the S3 Cross-Region Compressor.

This module provides functions for creating alarm resources to monitor
various aspects of the application, including DLQ message counts,
ECS task failures, and service utilization.
"""

from constructs import Construct
from aws_cdk import (
	Duration,
	aws_cloudwatch as cw,
	aws_cloudwatch_actions as cw_actions,
	aws_sns as sns,
	aws_sqs as sqs,
	aws_ecs as ecs,
)
from s3_cross_region_compressor.utils.ecs_utils import (
	create_running_task_count_metric,
	create_desired_count_metric,
)


def create_dlq_alarm(scope: Construct, id: str, dlq_queue: sqs.Queue, sns_topic: sns.Topic) -> cw.Alarm:
	"""
	Create an alarm that triggers when ANY message appears in a DLQ.

	Args:
	    scope: The CDK construct scope
	    id: Identifier for the alarm resources
	    dlq_queue: The DLQ to monitor
	    sns_topic: The SNS topic to notify

	Returns:
	    A CloudWatch alarm that triggers when any messages are in the DLQ
	"""
	alarm = dlq_queue.metric_approximate_number_of_messages_visible(
		period=Duration.minutes(1), statistic='Sum'
	).create_alarm(
		scope=scope,
		id=f'DLQAlarm-{id}',
		alarm_name=f'DLQ-Messages-{id}',
		alarm_description=f'Alarm when ANY message appears in DLQ {dlq_queue.queue_name}',
		threshold=0,
		comparison_operator=cw.ComparisonOperator.GREATER_THAN_THRESHOLD,
		evaluation_periods=1,
		treat_missing_data=cw.TreatMissingData.NOT_BREACHING,
	)

	# Add SNS action
	alarm.add_alarm_action(cw_actions.SnsAction(sns_topic))

	return alarm


def create_ecs_task_failures_alarm(
	scope: Construct, id: str, ecs_cluster: ecs.Cluster, ecs_service: ecs.FargateService, sns_topic: sns.Topic
) -> cw.Alarm:
	"""
	Create an alarm for ECS task failures.

	This alarm triggers when there's a consistent gap between desired and running tasks,
	indicating that tasks are failing to launch or are being terminated prematurely.

	Args:
	    scope: The CDK construct scope
	    id: Identifier for the alarm resources
	    ecs_cluster: The ECS cluster to monitor
	    ecs_service: The ECS service to monitor
	    sns_topic: The SNS topic to notify

	Returns:
	    A CloudWatch alarm that triggers when tasks are failing repeatedly
	"""
	# Create metrics for desired and running tasks
	desired_task_count = create_desired_count_metric(ecs_cluster, ecs_service)
	running_task_count = create_running_task_count_metric(ecs_cluster, ecs_service)

	# Create a math expression for the difference
	task_failure_expression = cw.MathExpression(
		expression='m1 - m2',
		using_metrics={'m1': desired_task_count, 'm2': running_task_count},
		label='Task failures (desired - running)',
		period=Duration.seconds(60),
	)

	# Create alarm for when difference persists
	alarm = cw.Alarm(
		scope=scope,
		id=f'TaskFailuresAlarm-{id}',
		alarm_name=f'ECS-Task-Failures-{id}',
		alarm_description=f'Alarm when ECS tasks are failing repeatedly for service {id}',
		metric=task_failure_expression,
		threshold=1,
		comparison_operator=cw.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
		evaluation_periods=3,  # Tasks failing for 3 consecutive periods (3 minutes)
		treat_missing_data=cw.TreatMissingData.NOT_BREACHING,
	)

	# Add SNS action
	alarm.add_alarm_action(cw_actions.SnsAction(sns_topic))

	return alarm


def create_max_capacity_alarm(
	scope: Construct,
	id: str,
	ecs_cluster: ecs.Cluster,
	ecs_service: ecs.FargateService,
	max_capacity: int,
	sns_topic: sns.Topic,
) -> cw.Alarm:
	"""
	Create an alarm that triggers when a service is at max capacity for extended periods.

	This alarm helps identify when a service needs to be right-sized to allow it to
	scale out wider than the current max capacity.

	Args:
	    scope: The CDK construct scope
	    id: Identifier for the alarm resources
	    ecs_cluster: The ECS cluster to monitor
	    ecs_service: The ECS service to monitor
	    max_capacity: The maximum capacity of the service
	    sns_topic: The SNS topic to notify

	Returns:
	    A CloudWatch alarm that triggers when service is at max capacity for extended periods
	"""
	# Get desired task count metric
	desired_task_count = create_desired_count_metric(ecs_cluster, ecs_service)

	# Create a math expression to check if desired count equals max capacity
	at_max_capacity_expression = cw.MathExpression(
		expression=f'm1 >= {max_capacity}',
		using_metrics={'m1': desired_task_count},
		label=f'At max capacity ({max_capacity} tasks)',
		period=Duration.seconds(60),
	)

	# Alarm when at max capacity for 15+ minutes
	alarm = cw.Alarm(
		scope=scope,
		id=f'MaxCapacityAlarm-{id}',
		alarm_name=f'Service-At-Max-Capacity-{id}',
		alarm_description=f'Alarm when service {id} is at maximum capacity for extended periods',
		metric=at_max_capacity_expression,
		threshold=0.5,  # Slightly less than 1 to account for potential metric variability
		comparison_operator=cw.ComparisonOperator.GREATER_THAN_THRESHOLD,
		evaluation_periods=15,  # 15 minutes at max capacity
		treat_missing_data=cw.TreatMissingData.NOT_BREACHING,
	)

	# Add SNS action
	alarm.add_alarm_action(cw_actions.SnsAction(sns_topic))

	return alarm
