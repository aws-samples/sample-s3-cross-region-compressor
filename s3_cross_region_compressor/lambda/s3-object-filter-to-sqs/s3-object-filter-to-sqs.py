import json
import boto3
import uuid
import logging
import os
from datetime import datetime
from typing import Dict, List, Any, Optional

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Lambda function to filter S3 objects and send filtered results to SQS as S3 events.
    
    Input format:
    {
        'BatchInput': {
            'execution': {
                'bucket': 'bucket-name',
                'prefix_filter': 'optional-prefix',
                'suffix_filter': 'optional-suffix', 
                'start_epoch': optional_timestamp,
                'end_epoch': optional_timestamp
            }
        },
        'Items': [
            {
                'Etag': '"etag-value"',
                'Key': 'object-key',
                'LastModified': epoch_timestamp,
                'Size': object_size,
                'StorageClass': 'STANDARD'
            }
        ]
    }
    """
    
    logger.info("Lambda function started")
    
    try:
        # Extract input parameters
        batch_input = event['BatchInput']['execution']
        items = event['Items']
        
        bucket_name = batch_input['bucket']
        prefix_filter = batch_input.get('prefix_filter')
        suffix_filter = batch_input.get('suffix_filter')
        start_epoch = batch_input.get('start_epoch')
        end_epoch = batch_input.get('end_epoch')
        
        logger.info(f"Processing {len(items)} objects from bucket: {bucket_name}")
        logger.info(f"Filters - prefix: {prefix_filter}, suffix: {suffix_filter}, start_epoch: {start_epoch}, end_epoch: {end_epoch}")
        
        # Filter objects based on criteria
        filtered_objects = filter_objects(items, prefix_filter, suffix_filter, start_epoch, end_epoch)
        
        objects_filtered_out = len(items) - len(filtered_objects)
        logger.info(f"Filtering complete: {len(filtered_objects)} objects passed filters, {objects_filtered_out} objects filtered out")
        
        if not filtered_objects:
            logger.info("No objects matched the filtering criteria - exiting")
            return {
                'statusCode': 200,
                'body': {
                    'message': 'No objects matched the filtering criteria',
                    'total_objects': len(items),
                    'filtered_objects': 0,
                    'messages_sent': 0
                }
            }
        
        # Process objects with multi-queue distribution
        logger.info(f"Starting multi-queue distribution for {len(filtered_objects)} objects")
        messages_sent, queue_distribution = send_to_multiple_sqs_queues(filtered_objects, bucket_name, prefix_filter)
        
        logger.info(f"Lambda function completed successfully: {messages_sent} total messages sent across {len(queue_distribution)} queues")
        
        return {
            'statusCode': 200,
            'body': {
                'message': 'Successfully processed objects',
                'total_objects': len(items),
                'filtered_objects': len(filtered_objects),
                'messages_sent': messages_sent,
                'queue_distribution': queue_distribution
            }
        }
        
    except KeyError as e:
        error_msg = f"Missing required input parameter: {str(e)}"
        logger.error(error_msg)
        return {
            'statusCode': 400,
            'body': {
                'error': error_msg,
                'message': 'Invalid input format'
            }
        }
    except Exception as e:
        error_msg = f"Unexpected error: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return {
            'statusCode': 500,
            'body': {
                'error': str(e),
                'message': 'Failed to process objects'
            }
        }

def construct_sqs_queue_url(bucket_name: str, prefix_filter: Optional[str] = None) -> str:
    """
    Construct SQS queue URL based on naming convention:
    - With prefix: source-{bucket_name}-{prefix}
    - Without prefix: source-{bucket_name}
    """
    
    # Get current AWS account ID and region from the execution context
    sts = boto3.client('sts')
    account_id = sts.get_caller_identity()['Account']
    region = boto3.Session().region_name
    
    # Construct queue name based on naming convention
    if prefix_filter:
        # Remove any slashes or special characters from prefix for queue name
        sanitized_prefix = prefix_filter.replace('/', '-').replace('_', '-').strip('-')
        queue_name = f"source-{bucket_name}-{sanitized_prefix}"
    else:
        queue_name = f"source-{bucket_name}"
    
    # Construct full SQS queue URL
    queue_url = f"https://sqs.{region}.amazonaws.com/{account_id}/{queue_name}"
    
    logger.info(f"Constructed queue - Name: {queue_name}, Account: {account_id}, Region: {region}")
    
    return queue_url

def discover_sqs_queue_url_from_ddb(bucket_name: str, runtime_prefix: Optional[str] = None) -> str:
    """
    Discover the correct SQS queue URL by querying DynamoDB parameters table.
    
    This function finds the appropriate queue by:
    1. Querying DDB for all configurations for the given bucket
    2. Matching the runtime prefix with deployment-time prefixes
    3. Constructing the queue URL based on the matched configuration
    
    Args:
        bucket_name: The S3 bucket name
        runtime_prefix: The runtime prefix filter (may be a sub-prefix)
        
    Returns:
        The correct SQS queue URL
        
    Raises:
        Exception: If no matching configuration is found or DDB query fails
    """
    
    # Get environment variables
    stack_name = os.environ.get('STACK_NAME', 's3-compressor')
    table_name = os.environ.get('REPLICATION_PARAMETERS_TABLE_NAME')
    
    if not table_name:
        logger.error("REPLICATION_PARAMETERS_TABLE_NAME environment variable not set")
        raise ValueError("REPLICATION_PARAMETERS_TABLE_NAME environment variable not set")
    
    logger.info(f"Discovering queue for bucket: {bucket_name}, runtime prefix: {runtime_prefix}")
    
    try:
        # Initialize DynamoDB client
        dynamodb = boto3.client('dynamodb')
        
        # Query DynamoDB for all parameter names starting with /{stack_name}/{bucket_name}
        query_prefix = f'/{stack_name}/{bucket_name}'
        
        response = dynamodb.scan(
            TableName=table_name,
            FilterExpression='begins_with(ParameterName, :prefix)',
            ExpressionAttributeValues={
                ':prefix': {'S': query_prefix}
            },
            ProjectionExpression='ParameterName'
        )
        
        if not response.get('Items'):
            logger.error(f"No configurations found for bucket: {bucket_name}")
            raise ValueError(f"No configurations found for bucket: {bucket_name}")
        
        # Extract configured prefixes from parameter names
        configured_prefixes = []
        for item in response['Items']:
            param_name = item['ParameterName']['S']
            
            # Extract prefix from parameter name
            # Format: /{stack_name}/{bucket_name} or /{stack_name}/{bucket_name}/{prefix}
            parts = param_name.split('/')
            if len(parts) == 3:  # /{stack_name}/{bucket_name}
                configured_prefixes.append('')  # No prefix (catch-all)
            elif len(parts) == 4:  # /{stack_name}/{bucket_name}/{prefix}
                configured_prefixes.append(parts[3])
            
        logger.info(f"Found configured prefixes: {configured_prefixes}")
        
        # Find the best matching prefix
        matched_prefix = find_best_matching_prefix(runtime_prefix, configured_prefixes)
        logger.info(f"Best matching prefix: '{matched_prefix}' for runtime prefix: '{runtime_prefix}'")
        
        # Construct the correct queue URL
        queue_url = construct_queue_url_from_prefix(bucket_name, matched_prefix)
        logger.info(f"Discovered queue URL: {queue_url}")
        
        return queue_url
        
    except Exception as e:
        logger.error(f"Error discovering queue URL from DDB: {str(e)}", exc_info=True)
        # Fallback to the original logic in case of DDB issues
        logger.warning("Falling back to original queue URL construction")
        return construct_sqs_queue_url(bucket_name, runtime_prefix)

def find_best_matching_prefix(runtime_prefix: Optional[str], configured_prefixes: List[str]) -> str:
    """
    Find the best matching configured prefix for the given runtime prefix.
    
    Matching priority:
    1. Exact match
    2. Parent prefix match (runtime prefix starts with configured prefix)
    3. Catch-all (empty configured prefix)
    4. First available prefix
    
    Args:
        runtime_prefix: The runtime prefix filter
        configured_prefixes: List of configured prefixes from deployment
        
    Returns:
        The best matching configured prefix
    """
    
    if not configured_prefixes:
        return ''
    
    # Normalize runtime prefix
    runtime_prefix = runtime_prefix or ''
    
    # 1. Look for exact match
    if runtime_prefix in configured_prefixes:
        logger.info(f"Found exact prefix match: '{runtime_prefix}'")
        return runtime_prefix
    
    # 2. Look for parent prefix match (runtime prefix starts with configured prefix)
    if runtime_prefix:
        for config_prefix in configured_prefixes:
            if config_prefix and runtime_prefix.startswith(config_prefix + '/'):
                logger.info(f"Found parent prefix match: '{config_prefix}' for runtime: '{runtime_prefix}'")
                return config_prefix
            elif config_prefix and runtime_prefix.startswith(config_prefix) and config_prefix != '':
                logger.info(f"Found prefix match: '{config_prefix}' for runtime: '{runtime_prefix}'")
                return config_prefix
    
    # 3. Look for catch-all (empty prefix)
    if '' in configured_prefixes:
        logger.info("Using catch-all configuration (empty prefix)")
        return ''
    
    # 4. Use first available prefix as fallback
    fallback_prefix = configured_prefixes[0]
    logger.warning(f"No suitable match found, using first available prefix: '{fallback_prefix}'")
    return fallback_prefix

def construct_queue_url_from_prefix(bucket_name: str, configured_prefix: str) -> str:
    """
    Construct the SQS queue URL based on the deployment configuration prefix.
    
    Args:
        bucket_name: The S3 bucket name
        configured_prefix: The configured prefix from deployment
        
    Returns:
        The SQS queue URL
    """
    
    # Get current AWS account ID and region
    sts = boto3.client('sts')
    account_id = sts.get_caller_identity()['Account']
    region = boto3.Session().region_name
    
    # Construct queue name based on deployment configuration
    if configured_prefix:
        queue_name = f"source-{bucket_name}-{configured_prefix}"
    else:
        queue_name = f"source-{bucket_name}"
    
    # Construct full SQS queue URL
    queue_url = f"https://sqs.{region}.amazonaws.com/{account_id}/{queue_name}"
    
    logger.info(f"Constructed queue URL - Name: {queue_name}, Account: {account_id}, Region: {region}")
    
    return queue_url

def filter_objects(items: List[Dict[str, Any]],
                  prefix_filter: Optional[str] = None,
                  suffix_filter: Optional[str] = None,
                  start_epoch: Optional[int] = None,
                  end_epoch: Optional[int] = None) -> List[Dict[str, Any]]:
    """Filter objects based on provided criteria."""
    
    filtered = []
    filter_stats = {
        'directory_objects': 0,
        'prefix_filtered': 0,
        'suffix_filtered': 0,
        'start_epoch_filtered': 0,
        'end_epoch_filtered': 0
    }
    
    logger.info("Starting object filtering process")
    
    for item in items:
        object_key = item['Key']
        last_modified = item['LastModified']
        
        # Skip S3 prefix objects (directories) that end with "/"
        if object_key.endswith('/'):
            filter_stats['directory_objects'] += 1
            continue
        
        # Apply prefix filter if specified
        if prefix_filter and not object_key.startswith(prefix_filter):
            filter_stats['prefix_filtered'] += 1
            continue
            
        # Apply suffix filter if specified  
        if suffix_filter and not object_key.endswith(suffix_filter):
            filter_stats['suffix_filtered'] += 1
            continue
            
        # Apply start_epoch filter if specified
        if start_epoch and last_modified < start_epoch:
            filter_stats['start_epoch_filtered'] += 1
            continue
            
        # Apply end_epoch filter if specified
        if end_epoch and last_modified > end_epoch:
            filter_stats['end_epoch_filtered'] += 1
            continue
            
        filtered.append(item)
    
    # Log filtering statistics
    logger.info(f"Filter statistics:")
    logger.info(f"  - Directory objects (ending with '/'): {filter_stats['directory_objects']}")
    logger.info(f"  - Filtered by prefix: {filter_stats['prefix_filtered']}")
    logger.info(f"  - Filtered by suffix: {filter_stats['suffix_filtered']}")
    logger.info(f"  - Filtered by start_epoch: {filter_stats['start_epoch_filtered']}")
    logger.info(f"  - Filtered by end_epoch: {filter_stats['end_epoch_filtered']}")
    logger.info(f"  - Objects passed all filters: {len(filtered)}")
    
    return filtered

def create_s3_event_message(s3_object: Dict[str, Any], bucket_name: str) -> Dict[str, Any]:
    """Create an S3 ObjectCreated:Put event message from S3 object metadata."""
    
    try:
        # Convert epoch timestamp to ISO format
        event_time = datetime.utcfromtimestamp(s3_object['LastModified']).strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        
        # Clean up ETag (remove quotes if present)
        etag = s3_object['Etag'].strip('"')
        
        return {
            "Records": [
                {
                    "eventVersion": "2.1",
                    "eventSource": "aws:s3",
                    "eventTime": event_time,
                    "eventName": "ObjectCreated:Put",
                    "userIdentity": {
                        "principalId": "EXAMPLE"
                    },
                    "requestParameters": {
                        "sourceIPAddress": "127.0.0.1"
                    },
                    "responseElements": {
                        "x-amz-request-id": f"EXAMPLE{uuid.uuid4().hex[:12]}",
                        "x-amz-id-2": f"EXAMPLE{uuid.uuid4().hex}"
                    },
                    "s3": {
                        "s3SchemaVersion": "1.0",
                        "configurationId": "testConfigRule",
                        "bucket": {
                            "name": bucket_name,
                            "ownerIdentity": {
                                "principalId": "EXAMPLE"
                            },
                            "arn": f"arn:aws:s3:::{bucket_name}"
                        },
                        "object": {
                            "key": s3_object['Key'],
                            "size": s3_object['Size'],
                            "eTag": etag,
                            "sequencer": f"0{uuid.uuid4().hex[:15].upper()}"
                        }
                    }
                }
            ]
        }
    except Exception as e:
        logger.error(f"Error creating S3 event message for object {s3_object.get('Key', 'unknown')}: {str(e)}")
        raise

def send_to_sqs_batch(objects: List[Dict[str, Any]], 
                     bucket_name: str, 
                     queue_url: str) -> int:
    """Send S3 objects to SQS in batches as S3 event messages."""
    
    sqs = boto3.client('sqs')
    total_sent = 0
    total_failed = 0
    batch_count = 0
    
    # Process in batches of 10 (SQS limit)
    batch_size = 10
    total_batches = (len(objects) + batch_size - 1) // batch_size
    logger.info(f"Processing {len(objects)} objects in {total_batches} batches of up to {batch_size} messages each")
    
    for i in range(0, len(objects), batch_size):
        batch = objects[i:i + batch_size]
        batch_count += 1
        
        try:
            # Prepare batch entries
            entries = []
            for obj in batch:
                s3_event = create_s3_event_message(obj, bucket_name)
                entries.append({
                    'Id': str(uuid.uuid4()),
                    'MessageBody': json.dumps(s3_event)
                })
            
            # Send batch to SQS
            logger.info(f"Sending batch {batch_count}/{total_batches} with {len(entries)} messages")
            response = sqs.send_message_batch(
                QueueUrl=queue_url,
                Entries=entries
            )
            
            # Count successful and failed messages
            successful_count = len(response.get('Successful', []))
            failed_count = len(response.get('Failed', []))
            
            total_sent += successful_count
            total_failed += failed_count
            
            logger.info(f"Batch {batch_count} results: {successful_count} successful, {failed_count} failed")
            
            # Log any failures with details
            if response.get('Failed'):
                for failure in response['Failed']:
                    logger.error(f"Failed message ID {failure.get('Id')}: {failure.get('Code')} - {failure.get('Message')}")
                    
        except Exception as e:
            logger.error(f"Error processing batch {batch_count}: {str(e)}", exc_info=True)
            total_failed += len(batch)
    
    logger.info(f"SQS batch processing complete: {total_sent} messages sent successfully, {total_failed} failed")
    return total_sent

def send_to_multiple_sqs_queues(objects: List[Dict[str, Any]], 
                               bucket_name: str, 
                               runtime_prefix_filter: Optional[str] = None) -> tuple[int, Dict[str, int]]:
    """
    Send S3 objects to multiple SQS queues based on configured prefixes.
    
    This function:
    1. Gets all configured prefixes for the bucket from DynamoDB
    2. Groups objects by their matching configured prefix
    3. Sends each group to the appropriate queue using batch operations
    4. Handles objects that don't match any configured prefix
    
    Args:
        objects: List of S3 object dictionaries
        bucket_name: The S3 bucket name
        runtime_prefix_filter: Optional runtime prefix filter for single-queue mode
        
    Returns:
        Tuple of (total_messages_sent, queue_distribution_dict)
    """
    
    logger.info(f"Starting multi-queue distribution for {len(objects)} objects")
    
    # If a specific prefix filter is provided, use single-queue mode
    if runtime_prefix_filter:
        logger.info(f"Runtime prefix filter specified: '{runtime_prefix_filter}', using single-queue mode")
        queue_url = discover_sqs_queue_url_from_ddb(bucket_name, runtime_prefix_filter)
        messages_sent = send_to_sqs_batch(objects, bucket_name, queue_url)
        queue_distribution = {queue_url: messages_sent}
        return messages_sent, queue_distribution
    
    # Get all configured prefixes for this bucket
    configured_prefixes = get_configured_prefixes_from_ddb(bucket_name)
    
    if not configured_prefixes:
        logger.error(f"No configured prefixes found for bucket: {bucket_name}")
        raise ValueError(f"No configured prefixes found for bucket: {bucket_name}")
    
    logger.info(f"Found {len(configured_prefixes)} configured prefixes: {configured_prefixes}")
    
    # Group objects by their matching configured prefix
    object_groups = group_objects_by_prefix(objects, configured_prefixes)
    
    # Send each group to its appropriate queue
    total_messages_sent = 0
    queue_distribution = {}
    
    for prefix, grouped_objects in object_groups.items():
        if not grouped_objects:
            continue
            
        logger.info(f"Processing {len(grouped_objects)} objects for prefix: '{prefix}'")
        
        # Get the queue URL for this prefix
        queue_url = construct_queue_url_from_prefix(bucket_name, prefix)
        
        # Send objects to the queue
        messages_sent = send_to_sqs_batch(grouped_objects, bucket_name, queue_url)
        
        total_messages_sent += messages_sent
        queue_distribution[queue_url] = messages_sent
        
        logger.info(f"Sent {messages_sent} messages to queue for prefix '{prefix}': {queue_url}")
    
    # Handle unmatched objects (if any)
    unmatched_objects = object_groups.get('UNMATCHED', [])
    if unmatched_objects:
        logger.warning(f"Found {len(unmatched_objects)} objects that don't match any configured prefix:")
        for obj in unmatched_objects[:5]:  # Log first 5 for debugging
            logger.warning(f"  - Unmatched object key: {obj['Key']}")
        if len(unmatched_objects) > 5:
            logger.warning(f"  - And {len(unmatched_objects) - 5} more unmatched objects...")
    
    logger.info(f"Multi-queue distribution complete: {total_messages_sent} total messages sent to {len(queue_distribution)} queues")
    return total_messages_sent, queue_distribution

def get_configured_prefixes_from_ddb(bucket_name: str) -> List[str]:
    """
    Get all configured prefixes for a bucket from DynamoDB parameters table.
    
    Args:
        bucket_name: The S3 bucket name
        
    Returns:
        List of configured prefixes (empty string for catch-all)
    """
    
    # Get environment variables
    stack_name = os.environ.get('STACK_NAME', 's3-compressor')
    table_name = os.environ.get('REPLICATION_PARAMETERS_TABLE_NAME')
    
    if not table_name:
        logger.error("REPLICATION_PARAMETERS_TABLE_NAME environment variable not set")
        raise ValueError("REPLICATION_PARAMETERS_TABLE_NAME environment variable not set")
    
    try:
        # Initialize DynamoDB client
        dynamodb = boto3.client('dynamodb')
        
        # Query DynamoDB for all parameter names starting with /{stack_name}/{bucket_name}
        query_prefix = f'/{stack_name}/{bucket_name}'
        
        response = dynamodb.scan(
            TableName=table_name,
            FilterExpression='begins_with(ParameterName, :prefix)',
            ExpressionAttributeValues={
                ':prefix': {'S': query_prefix}
            },
            ProjectionExpression='ParameterName'
        )
        
        if not response.get('Items'):
            logger.error(f"No configurations found for bucket: {bucket_name}")
            return []
        
        # Extract configured prefixes from parameter names
        configured_prefixes = []
        for item in response['Items']:
            param_name = item['ParameterName']['S']
            
            # Extract prefix from parameter name
            # Format: /{stack_name}/{bucket_name} or /{stack_name}/{bucket_name}/{prefix}
            parts = param_name.split('/')
            if len(parts) == 3:  # /{stack_name}/{bucket_name}
                configured_prefixes.append('')  # No prefix (catch-all)
            elif len(parts) == 4:  # /{stack_name}/{bucket_name}/{prefix}
                configured_prefixes.append(parts[3])
        
        return configured_prefixes
        
    except Exception as e:
        logger.error(f"Error getting configured prefixes from DDB: {str(e)}", exc_info=True)
        raise

def group_objects_by_prefix(objects: List[Dict[str, Any]], configured_prefixes: List[str]) -> Dict[str, List[Dict[str, Any]]]:
    """
    Group objects by their matching configured prefix.
    
    Args:
        objects: List of S3 object dictionaries
        configured_prefixes: List of configured prefixes from deployment
        
    Returns:
        Dictionary mapping prefixes to lists of objects, plus 'UNMATCHED' for unmatched objects
    """
    
    # Initialize groups for each configured prefix
    object_groups = {prefix: [] for prefix in configured_prefixes}
    object_groups['UNMATCHED'] = []
    
    # Sort configured prefixes by length (descending) to ensure longest matches first
    # This prevents shorter prefixes from matching objects that should go to longer prefixes
    sorted_prefixes = sorted([p for p in configured_prefixes if p], key=len, reverse=True)
    
    logger.info(f"Grouping {len(objects)} objects using prefix priority order: {sorted_prefixes}")
    
    for obj in objects:
        object_key = obj['Key']
        matched = False
        
        # Try to match against configured prefixes (longest first)
        for prefix in sorted_prefixes:
            if object_key.startswith(prefix + '/') or object_key.startswith(prefix):
                object_groups[prefix].append(obj)
                matched = True
                break
        
        # If no specific prefix matched, check for catch-all (empty prefix)
        if not matched and '' in configured_prefixes:
            object_groups[''].append(obj)
            matched = True
        
        # If still no match, add to unmatched
        if not matched:
            object_groups['UNMATCHED'].append(obj)
    
    # Log grouping statistics
    for prefix, group_objects in object_groups.items():
        if group_objects:
            logger.info(f"Prefix '{prefix}': {len(group_objects)} objects")
    
    return object_groups
