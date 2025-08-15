"""
ECS utility functions for autoscaling policies.

This module provides utility functions for creating and configuring ECS autoscaling policies
based on SQS queue depths and other CloudWatch metrics.
"""

from constructs import Construct
from aws_cdk import (
	Duration,
	aws_ecs as ecs,
	aws_sqs as sqs,
	aws_cloudwatch as cw,
	aws_cloudwatch_actions as cw_actions,
	aws_applicationautoscaling as appscaling,
)


def create_sqs_queue_visible_messages_metric(
	sqs_queue: sqs.Queue, statistic: str = 'Average', period_sec: int = 60
) -> cw.Metric:
	"""
	Create a CloudWatch metric for SQS queue visible messages.

	Args:
	    sqs_queue: The SQS queue to monitor
	    statistic: The statistic to use (default: "Average")
	    period_sec: The period in seconds (default: 60)

	Returns:
	    A CloudWatch metric for the number of visible messages in the queue
	"""
	return sqs_queue.metric_approximate_number_of_messages_visible(
		statistic=statistic, period=Duration.seconds(period_sec)
	)


def create_sqs_queue_in_flight_messages_metric(
	sqs_queue: sqs.Queue, statistic: str = 'Average', period_sec: int = 60
) -> cw.Metric:
	"""
	Create a CloudWatch metric for SQS queue in-flight messages.

	Args:
	    sqs_queue: The SQS queue to monitor
	    statistic: The statistic to use (default: "Average")
	    period_sec: The period in seconds (default: 60)

	Returns:
	    A CloudWatch metric for the number of in-flight messages in the queue
	"""
	return sqs_queue.metric_approximate_number_of_messages_not_visible(
		statistic=statistic, period=Duration.seconds(period_sec)
	)


def create_running_task_count_metric(ecs_cluster: ecs.Cluster, ecs_service: ecs.FargateService) -> cw.Metric:
	"""
	Create a CloudWatch metric for running ECS task count.

	Args:
	    ecs_cluster: The ECS cluster to monitor
	    ecs_service: The ECS service to monitor

	Returns:
	    A CloudWatch metric for the number of running tasks
	"""
	return cw.Metric(
		namespace='ECS/ContainerInsights',
		metric_name='RunningTaskCount',
		dimensions_map={
			'ClusterName': ecs_cluster.cluster_name,
			'ServiceName': ecs_service.service_name,
		},
		statistic='Average',
		period=Duration.seconds(60),
	)


def create_desired_count_metric(ecs_cluster: ecs.Cluster, ecs_service: ecs.FargateService) -> cw.Metric:
	"""
	Create a CloudWatch metric for desired ECS task count.

	Args:
	    ecs_cluster: The ECS cluster to monitor
	    ecs_service: The ECS service to monitor

	Returns:
	    A CloudWatch metric for the desired number of tasks
	"""
	return cw.Metric(
		namespace='ECS/ContainerInsights',
		metric_name='DesiredTaskCount',
		dimensions_map={
			'ClusterName': ecs_cluster.cluster_name,
			'ServiceName': ecs_service.service_name,
		},
		statistic='Average',
		period=Duration.seconds(60),
	)


def create_queue_empty_but_messages_in_flight(
	sqs_queue: sqs.Queue,
) -> cw.MathExpression:
	"""
	Create a math expression that detects when queue is empty, but there are messages in flight.
	This accommodate situations where the processing is too fast and the queue is fully in flight.

	Args:
	    sqs_queue: The SQS queue to monitor

	Returns:
	    A math expression that equals 1 when queue has no visible messages and also no in flight processing.
	"""
	return cw.MathExpression(
		expression='IF( m1 + m2 == 0, 1, 0)',  # If there are no messages in the queue (visible or otherwise), return 1 and trigger scale to 0 action.
		using_metrics={
			'm1': create_sqs_queue_visible_messages_metric(sqs_queue),
			'm2': create_sqs_queue_in_flight_messages_metric(sqs_queue),
		},
		label='Queue with messages and zero tasks',
		period=Duration.seconds(60),
	)


def create_low_backlog_and_multiple_tasks_expression(
	sqs_queue: sqs.Queue, running_task_count: cw.Metric, target_backlog_per_task: int
) -> cw.MathExpression:
	"""
	Create a math expression that detects when backlog is low but multiple tasks are running.

	Args:
	    sqs_queue: The SQS queue to monitor
	    running_task_count: The metric for running task count
	    target_backlog_per_task: Target number of messages per task

	Returns:
	    A math expression that equals 1 when backlog is low but multiple tasks are running
	"""
	return cw.MathExpression(
		expression=f'IF(m1/IF(FILL(m2,0) < 1, 1, m2) <= {target_backlog_per_task / 2}, IF(m2>1, 1, 0), 0)',
		using_metrics={
			'm1': create_sqs_queue_visible_messages_metric(sqs_queue),
			'm2': running_task_count,
		},
		label='Low backlog and multiple tasks',
		period=Duration.seconds(60),
	)


def create_backlog_per_task_expression(
	sqs_queue: sqs.Queue,
	running_task_count: cw.Metric,
	scaling_target_backlog_per_task: int,
) -> cw.MathExpression:
	"""
	Create a math expression that calculates backlog per task.

	The expression divides the number of messages in queue by the number of running tasks.
	Handles the case where there are no running tasks by using a default value of 1.

	Args:
	    sqs_queue: The SQS queue to monitor
	    running_task_count: The metric for running task count

	Returns:
	    A math expression that calculates backlog per task
	"""
	return cw.MathExpression(
		expression=f'IF(FILL(m2,0) < 1 AND m1 > 0 AND m1 < {scaling_target_backlog_per_task}, {scaling_target_backlog_per_task + 1}, m1/IF(FILL(m2,0) < 1, 1, m2))',
		using_metrics={
			'm1': create_sqs_queue_visible_messages_metric(sqs_queue),
			'm2': running_task_count,
		},
		label='Backlog per task',
		period=Duration.seconds(60),
	)


def create_high_backlog_per_task_alarm(
	scope: Construct,
	id: str,
	backlog_per_task: cw.MathExpression,
	scaling_target_backlog_per_task: int,
) -> cw.Alarm:
	"""
	Create an alarm that triggers when backlog per task is higher than threshold.

	Args:
	    scope: The CDK construct scope
	    backlog_per_task: Math expression for backlog per task
	    scaling_target_backlog_per_task: Target backlog per task

	Returns:
	    The created CloudWatch alarm
	"""
	return cw.Alarm(
		scope,
		'HighBacklogPerTaskAlarm',
		alarm_name=f'{id}-HighBacklogPerTaskAlarm',
		comparison_operator=cw.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
		threshold=scaling_target_backlog_per_task,
		evaluation_periods=1,
		metric=backlog_per_task,
	)


def create_queue_empty_alarm(scope: Construct, id: str, queue_empty_metric: cw.MathExpression) -> cw.Alarm:
	"""
	Create an alarm that triggers when queue is empty.

	Args:
	    scope: The CDK construct scope
	    queue_empty_metric: Math expression that equals 1 when queue is completely empty (no visible or in-flight messages)

	Returns:
	    The created CloudWatch alarm
	"""
	return cw.Alarm(
		scope,
		'QueueEmptyAlarm',
		alarm_name=f'{id}-QueueEmptyAlarm',
		comparison_operator=cw.ComparisonOperator.GREATER_THAN_THRESHOLD,
		threshold=0,
		evaluation_periods=2,  # Sustained empty queue for 2 periods
		metric=queue_empty_metric,
	)


def create_low_backlog_multiple_tasks_alarm(
	scope: Construct, id: str, low_backlog_and_multiple_tasks: cw.MathExpression
) -> cw.Alarm:
	"""
	Create an alarm that triggers when backlog is low but multiple tasks are running.

	Args:
	    scope: The CDK construct scope
	    low_backlog_and_multiple_tasks: Math expression for low backlog but multiple tasks

	Returns:
	    The created CloudWatch alarm
	"""
	return cw.Alarm(
		scope,
		'LowBacklogMultipleTasksAlarm',
		alarm_name=f'{id}-LowBacklogMultipleTasksAlarm',
		comparison_operator=cw.ComparisonOperator.GREATER_THAN_THRESHOLD,
		threshold=0,  # Alarm when expression equals 1
		evaluation_periods=1,  # Sustained for 1 periods
		metric=low_backlog_and_multiple_tasks,
	)


def create_scale_out_action(
	scope: Construct,
	scaling: appscaling.ScalableTarget,
	scale_out_cooldown: int,
	scaling_target_backlog_per_task: int,
) -> appscaling.StepScalingAction:
	"""
	Create a scale-out action with steps based on backlog depth.

	Args:
	    scope: The CDK construct scope
	    scaling: The scalable target
	    scale_out_cooldown: Cooldown period in seconds
	    scaling_target_backlog_per_task: Target backlog per task

	Returns:
	    The created step scaling action
	"""
	scale_out = appscaling.StepScalingAction(
		scope,
		'ScaleOut',
		scaling_target=scaling,
		adjustment_type=appscaling.AdjustmentType.CHANGE_IN_CAPACITY,
		cooldown=Duration.seconds(scale_out_cooldown),
	)

	# This is a bit confusing, but the threshold doesn't directly behave as intuitively expected.
	# From CloudFormation documentation:

	# MetricIntervalLowerBound
	# The lower bound for the difference between the alarm threshold and the CloudWatch metric. If the metric value is above the breach threshold,
	# the lower bound is inclusive (the metric must be greater than or equal to the threshold plus the lower bound). Otherwise, it is exclusive
	# (the metric must be greater than the threshold plus the lower bound). A null value indicates negative infinity.

	# MetricIntervalUpperBound
	# The upper bound for the difference between the alarm threshold and the CloudWatch metric. If the metric value is above the breach threshold,
	# the upper bound is exclusive (the metric must be less than the threshold plus the upper bound). Otherwise, it is inclusive
	# (the metric must be less than or equal to the threshold plus the upper bound). A null value indicates positive infinity.

	# Speed up scaling the bigger the backlog is
	scale_out.add_adjustment(
		adjustment=1,
		lower_bound=0,
		upper_bound=scaling_target_backlog_per_task,
	)
	scale_out.add_adjustment(
		adjustment=2,
		lower_bound=scaling_target_backlog_per_task,
		upper_bound=scaling_target_backlog_per_task * 2,
	)
	scale_out.add_adjustment(
		adjustment=3,
		lower_bound=scaling_target_backlog_per_task * 2,
		upper_bound=scaling_target_backlog_per_task * 3,
	)
	scale_out.add_adjustment(
		adjustment=4,
		lower_bound=scaling_target_backlog_per_task * 3,
		upper_bound=scaling_target_backlog_per_task * 4,
	)
	scale_out.add_adjustment(
		adjustment=5,
		lower_bound=scaling_target_backlog_per_task * 4,
		upper_bound=scaling_target_backlog_per_task * 5,
	)
	scale_out.add_adjustment(
		adjustment=6,
		lower_bound=scaling_target_backlog_per_task * 5,
		upper_bound=scaling_target_backlog_per_task * 6,
	)
	scale_out.add_adjustment(
		adjustment=7,
		lower_bound=scaling_target_backlog_per_task * 6,
		upper_bound=scaling_target_backlog_per_task * 7,
	)
	scale_out.add_adjustment(
		adjustment=8,
		lower_bound=scaling_target_backlog_per_task * 7,
		upper_bound=scaling_target_backlog_per_task * 8,
	)
	scale_out.add_adjustment(adjustment=10, lower_bound=scaling_target_backlog_per_task * 8)

	return scale_out


def create_scale_to_zero_action(
	scope: Construct, scaling: appscaling.ScalableTarget, scale_in_cooldown: int
) -> appscaling.StepScalingAction:
	"""
	Create a scaling action that scales to zero when triggered.

	Args:
	    scope: The CDK construct scope
	    scaling: The scalable target
	    scale_in_cooldown: Cooldown period in seconds

	Returns:
	    The created step scaling action
	"""
	scale_to_zero = appscaling.StepScalingAction(
		scope,
		'ScaleToZero',
		scaling_target=scaling,
		adjustment_type=appscaling.AdjustmentType.EXACT_CAPACITY,
		cooldown=Duration.seconds(scale_in_cooldown),
	)

	# Required to have at least 2 intervals
	scale_to_zero.add_adjustment(adjustment=0, lower_bound=0)

	return scale_to_zero


def create_scale_in_action(
	scope: Construct, scaling: appscaling.ScalableTarget, scale_in_cooldown: int
) -> appscaling.StepScalingAction:
	"""
	Create a scale-in action that reduces capacity by one.

	Args:
	    scope: The CDK construct scope
	    scaling: The scalable target
	    scale_in_cooldown: Cooldown period in seconds

	Returns:
	    The created step scaling action
	"""
	scale_in = appscaling.StepScalingAction(
		scope,
		'ScaleIn',
		scaling_target=scaling,
		adjustment_type=appscaling.AdjustmentType.CHANGE_IN_CAPACITY,
		cooldown=Duration.seconds(scale_in_cooldown),
	)

	scale_in.add_adjustment(adjustment=0, upper_bound=0)  # No change above threshold
	scale_in.add_adjustment(adjustment=-1, lower_bound=0)  # Scale in by 1 when alarm is triggered

	return scale_in


def configure_scale_out_on_high_backlog(
	scope: Construct,
	id: str,
	scaling: appscaling.ScalableTarget,
	sqs_queue: sqs.Queue,
	running_task_count: cw.Metric,
	scaling_target_backlog_per_task: int,
	scale_out_cooldown: int,
) -> None:
	"""
	Configure scaling out when backlog per task is high.

	Args:
	    scope: The CDK construct scope
	    scaling: The scalable target
	    sqs_queue: The SQS queue to monitor
	    running_task_count: Metric for running task count
	    scaling_target_backlog_per_task: Target backlog per task
	    scale_out_cooldown: Cooldown period in seconds
	"""
	backlog_per_task = create_backlog_per_task_expression(
		sqs_queue, running_task_count, scaling_target_backlog_per_task
	)
	alarm = create_high_backlog_per_task_alarm(
		scope=scope,
		id=id,
		backlog_per_task=backlog_per_task,
		scaling_target_backlog_per_task=scaling_target_backlog_per_task,
	)
	action = create_scale_out_action(scope, scaling, scale_out_cooldown, scaling_target_backlog_per_task)
	alarm.add_alarm_action(cw_actions.ApplicationScalingAction(action))


def configure_scale_to_zero_on_empty_queue(
	scope: Construct,
	id: str,
	scaling: appscaling.ScalableTarget,
	sqs_queue: sqs.Queue,
	scale_in_cooldown: int,
) -> None:
	"""
	Configure scaling to zero when queue is empty.

	Args:
	    scope: The CDK construct scope
	    scaling: The scalable target
	    sqs_queue: The SQS queue to monitor
	    scale_in_cooldown: Cooldown period in seconds
	"""
	queue_empty_metric = create_queue_empty_but_messages_in_flight(sqs_queue)
	alarm = create_queue_empty_alarm(scope=scope, id=id, queue_empty_metric=queue_empty_metric)
	action = create_scale_to_zero_action(scope, scaling, scale_in_cooldown)
	alarm.add_alarm_action(cw_actions.ApplicationScalingAction(action))


def configure_scale_in_on_low_backlog(
	scope: Construct,
	id: str,
	scaling: appscaling.ScalableTarget,
	sqs_queue: sqs.Queue,
	running_task_count: cw.Metric,
	scaling_target_backlog_per_task: int,
	scale_in_cooldown: int,
) -> None:
	"""
	Configure scaling in when backlog is low.

	Args:
	    scope: The CDK construct scope
	    scaling: The scalable target
	    sqs_queue: The SQS queue to monitor
	    running_task_count: Metric for running task count
	    scaling_target_backlog_per_task: Target backlog per task
	    scale_in_cooldown: Cooldown period in seconds
	"""
	expression = create_low_backlog_and_multiple_tasks_expression(
		sqs_queue, running_task_count, scaling_target_backlog_per_task
	)
	alarm = create_low_backlog_multiple_tasks_alarm(scope=scope, id=id, low_backlog_and_multiple_tasks=expression)
	action = create_scale_in_action(scope, scaling, scale_in_cooldown)
	alarm.add_alarm_action(cw_actions.ApplicationScalingAction(action))


def create_autoscaling_policy(
	scope,
	id: str,
	ecs_cluster: ecs.Cluster,
	ecs_service: ecs.FargateService,
	sqs_queue: sqs.Queue,
	min_capacity: int = 0,
	max_capacity: int = 20,
	scaling_target_backlog_per_task: int = 60,
	scale_out_cooldown: int = 60,
	scale_in_cooldown: int = 90,
):
	"""
	Create a comprehensive auto-scaling policy for an ECS service based on SQS queue depth.

	This configures several scaling policies:
	1. Scale out when backlog exceeds target per task
	2. Scale to zero when queue is empty
	3. Scale from zero when messages appear but no tasks are running
	4. Scale in when backlog is low but multiple tasks are running

	Args:
	    scope: The CDK construct scope
	    id: The ID for resources
	    ecs_cluster: The ECS cluster
	    ecs_service: The ECS service to scale
	    sqs_queue: The SQS queue to monitor for scaling decisions
	    min_capacity: Minimum number of tasks (default: 0)
	    max_capacity: Maximum number of tasks (default: 20)
	    scaling_target_backlog_per_task: Target backlog per task (default: 60)
	    scale_out_cooldown: Cooldown period for scaling out in seconds (default: 60)
	    scale_in_cooldown: Cooldown period for scaling in in seconds (default: 90)
	"""
	# Create scalable target
	scaling = appscaling.ScalableTarget(
		scope=scope,
		id=f'ScalableTarget-{id}',
		service_namespace=appscaling.ServiceNamespace.ECS,
		max_capacity=max_capacity,
		min_capacity=min_capacity,
		resource_id=f'service/{ecs_cluster.cluster_name}/{ecs_service.service_name}',
		scalable_dimension='ecs:service:DesiredCount',
	)

	# Create metrics
	# sqs_queue_length = create_sqs_queue_visible_messages_metric(sqs_queue)
	running_task_count = create_running_task_count_metric(ecs_cluster=ecs_cluster, ecs_service=ecs_service)
	# create_desired_count_metric(ecs_cluster=ecs_cluster, ecs_service=ecs_service)

	# Configure scaling strategies
	configure_scale_out_on_high_backlog(
		scope=scope,
		id=id,
		scaling=scaling,
		sqs_queue=sqs_queue,
		running_task_count=running_task_count,
		scaling_target_backlog_per_task=scaling_target_backlog_per_task,
		scale_out_cooldown=scale_out_cooldown,
	)

	configure_scale_to_zero_on_empty_queue(
		scope=scope, id=id, scaling=scaling, sqs_queue=sqs_queue, scale_in_cooldown=scale_in_cooldown
	)

	configure_scale_in_on_low_backlog(
		scope=scope,
		id=id,
		scaling=scaling,
		sqs_queue=sqs_queue,
		running_task_count=running_task_count,
		scaling_target_backlog_per_task=scaling_target_backlog_per_task,
		scale_in_cooldown=scale_in_cooldown,
	)
