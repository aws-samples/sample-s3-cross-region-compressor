"""
Utility functions for S3 buckets and operations.

This module provides helper functions for working with S3 buckets,
such as adding event notifications and creating bucket references.
"""

from typing import Optional
from constructs import Construct
from aws_cdk import aws_s3 as s3, aws_s3_notifications as s3n, aws_sqs as sqs


def build_notification_filter(prefix: str, suffix: str) -> Optional[s3.NotificationKeyFilter]:
	"""
	Build an S3 notification key filter based on prefix and suffix values.

	Creates a filter for S3 event notifications to limit which objects
	trigger notifications based on their key prefix and/or suffix.

	Args:
	    prefix: The prefix to filter objects (can be empty)
	    suffix: The suffix to filter objects (can be empty)

	Returns:
	    NotificationKeyFilter if either prefix or suffix is set, None otherwise
	"""
	if not prefix and not suffix:
		return None

	filter_args = {}
	if prefix:
		# Ensure prefix ends with '/' for proper S3 notification filtering
		normalized_prefix = prefix if prefix.endswith('/') else f"{prefix}/"
		filter_args['prefix'] = normalized_prefix
	if suffix:
		filter_args['suffix'] = suffix

	return s3.NotificationKeyFilter(**filter_args) if filter_args else None


def add_source_bucket_notification(
	scope: Construct,
	bucket_name: str,
	sqs_queue: sqs.Queue,
	prefix_filter: str,
	suffix_filter: str,
) -> None:
	"""
	Add an S3 event notification to a bucket for OBJECT_CREATED events.

	Configures a source S3 bucket to send event notifications to an SQS queue
	when objects are created, optionally filtered by prefix and/or suffix.

	Args:
	    scope: The CDK construct scope
	    bucket_name: Name of the S3 bucket
	    sqs_queue: SQS queue to receive notifications
	    prefix_filter: Prefix filter for object keys (can be empty)
	    suffix_filter: Suffix filter for object keys (can be empty)
	"""
	# Create the bucket reference
	source_bucket = s3.Bucket.from_bucket_name(scope, 'source_bucket', bucket_name)

	notification_filter = build_notification_filter(prefix_filter, suffix_filter)

	if notification_filter:
		source_bucket.add_event_notification(
			s3.EventType.OBJECT_CREATED, s3n.SqsDestination(sqs_queue), notification_filter
		)
	else:
		source_bucket.add_event_notification(s3.EventType.OBJECT_CREATED, s3n.SqsDestination(sqs_queue))


def add_inbound_bucket_notification(
	scope: Construct,
	bucket_name: str,
	sqs_queue: sqs.Queue,
) -> None:
	"""
	Add an S3 event notification to a bucket for OBJECT_CREATED events.

	Configures an inbound S3 bucket to send event notifications to an SQS queue
	when objects are created.

	Args:
	    scope: The CDK construct scope
	    bucket_name: Name of the S3 bucket
	    sqs_queue: SQS queue to receive notifications
	"""

	source_bucket = s3.Bucket.from_bucket_name(scope, 'inbound_bucket', bucket_name)
	source_bucket.add_event_notification(s3.EventType.OBJECT_CREATED, s3n.SqsDestination(sqs_queue))


def add_replication_rule(
	prefix: str,
	destination: str,
	target_region: str,
	account_id: str,
	rule_priority: int,
):
	"""
	Create an S3 replication rule for cross-region replication.

	This function creates a rule configuration for S3 replication between
	a source bucket and a destination bucket in another region.

	Args:
	    prefix (str): Prefix filter for objects to replicate
	    destination (str): Name of the destination S3 bucket
	    target_region (str): AWS region of the destination bucket
	    account_id (str): AWS account ID
	    rule_priority (int): Priority of the replication rule

	Returns:
	    dict: A replication rule configuration for use in putBucketReplication API
	"""
	kms_key_arn = f'arn:aws:kms:{target_region}:{account_id}:alias/inbound'
	bucket = f'arn:aws:s3:::{destination}'
	rule = {
		'Status': 'Enabled',
		'Filter': {'Prefix': prefix},
		'Priority': rule_priority,
		'Destination': {
			'Bucket': bucket,
			'EncryptionConfiguration': {'ReplicaKmsKeyID': kms_key_arn},
		},
		'DeleteMarkerReplication': {'Status': 'Enabled'},
		'SourceSelectionCriteria': {'SseKmsEncryptedObjects': {'Status': 'Enabled'}},
	}
	return rule
