"""
Notification-related resource creation for the S3 Cross-Region Compressor.

This module provides functions for creating notification resources,
such as S3 event notifications and SNS topics for alerts and monitoring.
"""

from typing import List
from constructs import Construct
from aws_cdk import aws_sqs as sqs, aws_sns as sns, aws_kms as kms
from s3_cross_region_compressor.utils.s3_utils import add_source_bucket_notification


def configure_source_bucket_notifications(
	scope: Construct, replication_config: list, region: str, sqs_queue: sqs.Queue
) -> None:
	"""
	Configure S3 event notifications for source buckets.

	Configures source S3 buckets to send event notifications to an SQS queue
	when objects are created, based on the replication configuration.

	Args:
	    scope: The CDK construct scope
	    replication_config: Configuration for S3 bucket replication
	    region: Current AWS region
	    sqs_queue: SQS queue to receive notifications
	"""
	for source_config in replication_config:
		source_config = source_config['source']
		if source_config['region'] == region:
			add_source_bucket_notification(
				scope,
				source_config['bucket'],
				sqs_queue,
				source_config['prefix_filter'],
				source_config['suffix_filter'],
			)


def create_alarm_topic(scope: Construct, stack_name: str, notification_emails: List[str], kms_key: kms.Key) -> sns.Topic:
	"""
	Create an SNS topic for alarms and subscribe all provided email addresses.

	This function creates an SNS topic that will be used for sending alarm notifications
	and subscribes all email addresses from the notification_emails list to this topic.

	Args:
	    scope: The CDK construct scope
	    stack_name: Name of the stack for resource naming
	    notification_emails: List of email addresses to subscribe to the topic

	Returns:
	    An SNS topic configured with email subscriptions
	"""
	topic = sns.Topic(
		scope=scope, id='AlarmTopic', display_name=f'{stack_name}-Alarms', topic_name=f'{stack_name}-alarms', master_key=kms_key
	)

	# Subscribe all emails to the topic
	for i, email in enumerate(notification_emails):
		sns.Subscription(
			scope=scope,
			id=f'EmailSubscription-{i}',
			topic=topic,
			protocol=sns.SubscriptionProtocol.EMAIL,
			endpoint=email,
		)

	return topic
