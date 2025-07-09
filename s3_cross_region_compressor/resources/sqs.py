"""
Messaging-related resource creation for the S3 Cross-Region Compressor.

This module provides functions for creating messaging resources,
such as SQS queues with appropriate security settings.
"""

from constructs import Construct
from aws_cdk import Duration, RemovalPolicy, aws_sqs as sqs, aws_kms as kms


def create_sqs_queue(scope: Construct, kms_key: kms.Key, sqs_id: str, visibility_timeout: int = 300) -> sqs.Queue:
	"""
	Create an SQS queue with KMS encryption and dead-letter queue.

	Creates an SQS queue for receiving S3 event notifications, with
	KMS encryption, SSL enforcement, and a dead-letter queue for
	handling failed message processing.

	Args:
	    scope: The CDK construct scope
	    kms_key: The KMS key to use for encryption
	    sqs_id: Identifier for the SQS queue

	Returns:
	    sqs.Queue: The created SQS queue
	"""
	sqs_dlq_queue = sqs.Queue(
		scope=scope,
		id=f'{sqs_id}-sqs-dlq',
		queue_name=f'{sqs_id}-dlq',
		visibility_timeout=Duration.seconds(visibility_timeout),
		encryption=sqs.QueueEncryption.KMS,
		encryption_master_key=kms_key,
		enforce_ssl=True,
		removal_policy=RemovalPolicy.DESTROY,
	)

	return sqs_dlq_queue, sqs.Queue(
		scope=scope,
		id=f'{sqs_id}-sqs',
		visibility_timeout=Duration.seconds(visibility_timeout),
		queue_name=sqs_id,
		dead_letter_queue=sqs.DeadLetterQueue(
			max_receive_count=5,
			queue=sqs_dlq_queue,
		),
		encryption=sqs.QueueEncryption.KMS,
		encryption_master_key=kms_key,
		enforce_ssl=True,
		removal_policy=RemovalPolicy.DESTROY,
	)
