"""
AWS Utilities for Target Region Container

This module provides utility functions for interacting with AWS services:
- SQS: Message batch processing
- S3: Object retrieval, upload, and deletion
- CloudWatch: Metrics reporting
- S3 Event Detection: Identification of test events
"""

import json
import logging
import os
import sys
from typing import Dict, List, Optional, Tuple
from urllib.parse import unquote_plus

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

# Configure logging
logger = logging.getLogger(__name__)

# Configure boto3 session with increased connection pool size
# Default is 10 connections, increasing to 50
boto_config = Config(max_pool_connections=50, retries={'max_attempts': 3})

# Create a session with the custom configuration
session = boto3.session.Session()

# Initialize AWS clients with the custom configuration
s3_client = session.client('s3', config=boto_config)
sqs_client = session.client('sqs', config=boto_config)
cloudwatch_client = session.client('cloudwatch', config=boto_config)


def get_sqs_messages(queue_url: str, max_messages: int = 1, visibility_timeout: int = 300) -> List[Dict]:
	"""
	Retrieve a batch of messages from an SQS queue.

	Args:
	    queue_url: URL of the SQS queue
	    max_messages: Maximum number of messages to retrieve (1-10)
	    visibility_timeout: Visibility timeout in seconds

	Returns:
	    List of message dictionaries
	"""
	try:
		response = sqs_client.receive_message(
			QueueUrl=queue_url,
			MaxNumberOfMessages=max_messages,
			VisibilityTimeout=visibility_timeout,
			WaitTimeSeconds=20,  # Long polling
		)

		return response.get('Messages', [])
	except ClientError as e:
		logger.error(f'Error retrieving SQS messages: {e}')
		return []


def delete_sqs_message(queue_url: str, receipt_handle: str) -> bool:
	"""
	Delete a message from an SQS queue.

	Args:
	    queue_url: URL of the SQS queue
	    receipt_handle: Receipt handle of the message to delete

	Returns:
	    True if successful, False otherwise
	"""
	try:
		sqs_client.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
		return True
	except ClientError as e:
		logger.error(f'Error deleting SQS message: {e}')
		return False


def delete_sqs_messages_batch(queue_url: str, receipt_handles: List[str]) -> Tuple[List[str], List[str]]:
	"""
	Delete multiple messages from an SQS queue in a batch.

	Args:
	    queue_url: URL of the SQS queue
	    receipt_handles: List of receipt handles to delete

	Returns:
	    Tuple of (successful_ids, failed_ids)
	"""
	if not receipt_handles:
		return [], []

	entries = [{'Id': str(i), 'ReceiptHandle': rh} for i, rh in enumerate(receipt_handles)]

	try:
		response = sqs_client.delete_message_batch(QueueUrl=queue_url, Entries=entries)

		successful_ids = [entry['Id'] for entry in response.get('Successful', [])]
		failed_ids = [entry['Id'] for entry in response.get('Failed', [])]

		if failed_ids:
			logger.warning(f'Failed to delete {len(failed_ids)} messages from SQS queue')

		return successful_ids, failed_ids
	except ClientError as e:
		logger.error(f'Error batch deleting SQS messages: {e}')
		return [], [str(i) for i in range(len(receipt_handles))]


def is_s3_test_event(message: Dict) -> bool:
	"""
	Detect if an SQS message contains an S3 test event.

	When S3 event notifications are first configured, AWS sends a test event
	to verify the notification setup. These events should be identified and
	filtered out to prevent unnecessary processing.

	Args:
	    message: SQS message dictionary

	Returns:
	    True if the message is an S3 test event, False otherwise
	"""
	try:
		body = json.loads(message.get('Body', '{}'))

		# Check for the presence of 'Event' field with 's3:TestEvent' value
		if body.get('Event') == 's3:TestEvent':
			logger.debug('Detected S3 test event in message')
			return True

		# Also check if this is in the 'Records' array format but has a test event
		records = body.get('Records', [])
		for record in records:
			if record.get('eventSource') == 'aws:s3' and record.get('eventName') == 's3:TestEvent':
				logger.debug('Detected S3 test event in Records format')
				return True

		return False
	except (json.JSONDecodeError, KeyError) as e:
		logger.error(f'Error parsing message for test event detection: {e}')
		return False


def extract_s3_event_info(message: Dict) -> List[Dict]:
	"""
	Extract S3 event information from an SQS message.

	Args:
	    message: SQS message dictionary

	Returns:
	    List of dictionaries with bucket and key information
	"""
	try:
		body = json.loads(message.get('Body', '{}'))
		records = body.get('Records', [])

		s3_objects = []
		for record in records:
			if record.get('eventSource') == 'aws:s3' and record.get('eventName', '').startswith('ObjectCreated'):
				s3_info = record.get('s3', {})
				bucket = s3_info.get('bucket', {}).get('name')
				key = s3_info.get('object', {}).get('key')

				if bucket and key:
					# URL decode the key to handle spaces and special characters
					decoded_key = unquote_plus(key)
					s3_objects.append({'bucket': bucket, 'key': decoded_key})

		return s3_objects
	except (json.JSONDecodeError, KeyError) as e:
		logger.error(f'Error extracting S3 event info: {e}')
		return []


def get_s3_object(bucket: str, key: str, local_path: str) -> bool:
	"""
	Download an S3 object to a local file.
	Enhanced for non-root user execution.

	Args:
	    bucket: S3 bucket name
	    key: S3 object key
	    local_path: Local file path to save the object

	Returns:
	    True if successful, False otherwise
	"""
	try:
		s3_client.download_file(bucket, key, local_path)
		
		# Ensure the downloaded file is readable/writable by the current user
		try:
			os.chmod(local_path, 0o644)
			logger.debug(f"Set file permissions for {local_path}")
		except Exception as chmod_e:
			logger.warning(f"Could not set file permissions for {local_path}: {chmod_e}")
			# Check if file is still accessible
			if not os.access(local_path, os.R_OK | os.W_OK):
				logger.error(f"Downloaded file {local_path} is not accessible")
				return False
		
		return True
	except ClientError as e:
		logger.error(f'Error downloading S3 object {bucket}/{key}: {e}')
		return False


def upload_to_s3(
	local_path: str,
	bucket: str,
	key: str,
	tags: Optional[Dict[str, str]] = None,
	storage_class: Optional[str] = None,
	kms_key_arn: Optional[str] = None,
) -> bool:
	"""
	Upload a local file to S3 with optional tags, storage class, and KMS encryption.

	Args:
	    local_path: Local file path
	    bucket: S3 bucket name
	    key: S3 object key
	    tags: Optional dictionary of tags to apply
	    storage_class: Optional storage class for the object (e.g., 'STANDARD', 'STANDARD_IA', etc.)
	    kms_key_arn: Optional KMS key ARN for server-side encryption

	Returns:
	    True if successful, False otherwise
	"""
	try:
		# Prepare extra args for upload
		extra_args = {}

		# Add storage class if provided
		if storage_class:
			extra_args['StorageClass'] = storage_class
			logger.debug(f'Setting storage class to {storage_class} for {bucket}/{key}')

		# Add KMS encryption if a key ARN is provided
		if kms_key_arn:
			extra_args['ServerSideEncryption'] = 'aws:kms'
			extra_args['SSEKMSKeyId'] = kms_key_arn
			logger.debug(f'Using KMS encryption with key: {kms_key_arn} for {bucket}/{key}')

		# Log the complete extra_args for debugging
		logger.debug(f'S3 upload ExtraArgs: {extra_args} for {bucket}/{key}')

		# Upload the file with extra args
		s3_client.upload_file(local_path, bucket, key, ExtraArgs=extra_args)

		# Apply tags if provided
		if tags:
			tag_set = [{'Key': k, 'Value': v} for k, v in tags.items()]
			s3_client.put_object_tagging(Bucket=bucket, Key=key, Tagging={'TagSet': tag_set})

		return True
	except ClientError as e:
		logger.error(f'Error uploading file to S3 {bucket}/{key}: {e}')
		return False


def delete_s3_object(bucket: str, key: str) -> bool:
	"""
	Delete an S3 object.

	Args:
	    bucket: S3 bucket name
	    key: S3 object key

	Returns:
	    True if successful, False otherwise
	"""
	try:
		s3_client.delete_object(Bucket=bucket, Key=key)
		return True
	except ClientError as e:
		logger.error(f'Error deleting S3 object {bucket}/{key}: {e}')
		return False


def put_cloudwatch_metric(
	namespace: str,
	metric_name: str,
	value: float,
	unit: str,
	dimensions: List[Dict[str, str]],
) -> bool:
	"""
	Put a metric data point to CloudWatch.

	Args:
	    namespace: Metric namespace
	    metric_name: Metric name
	    value: Metric value
	    unit: Metric unit
	    dimensions: List of dimension dictionaries

	Returns:
	    True if successful, False otherwise
	"""
	try:
		cloudwatch_client.put_metric_data(
			Namespace=namespace,
			MetricData=[
				{
					'MetricName': metric_name,
					'Value': value,
					'Unit': unit,
					'Dimensions': dimensions,
				}
			],
		)
		return True
	except ClientError as e:
		logger.error(f'Error putting CloudWatch metric {metric_name}: {e}')
		return False


def get_env_var(name: str, required: bool = True) -> Optional[str]:
	"""
	Get an environment variable.

	Args:
	    name: Name of the environment variable
	    required: Whether the variable is required

	Returns:
	    Value of the environment variable or None if not found and not required
	"""
	value = os.environ.get(name)

	if required and not value:
		logger.error(f'Required environment variable {name} not set')
		sys.exit(1)

	return value


def get_current_region() -> str:
	"""
	Get the current AWS region from the environment.

	Returns:
	    Current AWS region
	"""
	# Try to get from environment variable
	region = os.environ.get('AWS_DEFAULT_REGION')

	# Fall back to boto3 session if not in environment
	if not region:
		session = boto3.session.Session()
		region = session.region_name

	# Default to us-east-1 if still not found
	if not region:
		logger.warning('Could not determine AWS region, defaulting to us-east-1')
		region = 'us-east-1'

	return region
