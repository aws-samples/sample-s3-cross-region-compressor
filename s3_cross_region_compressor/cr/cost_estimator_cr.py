"""
AWS Lambda function for a CDK Custom Resource that calculates Fargate and data transfer costs.

This function calculates:
1. The per-minute cost of Fargate ARM SPOT instances given the region, CPU, memory, and ephemeral storage
2. The average cost of data transfer from the source region to the specified target regions
"""

import boto3
import json
import logging
import urllib.request

#solve vuln B310
import urllib.parse

from decimal import Decimal
from typing import Dict, List, Tuple, Optional, Any

# Setup logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# URL for Fargate spot pricing
FARGATE_PRICING_URL = 'https://dftu77xade0tc.cloudfront.net/fargate-spot-prices.json'


def get_fargate_spot_pricing(region: str) -> Tuple[Optional[Decimal], Optional[Decimal]]:
	"""
	Get Fargate Spot pricing for a specific region from the pricing JSON file.

	Args:
	    region: AWS region code (e.g., 'us-east-1')

	Returns:
	    tuple: (vCPU price per hour, Memory price per GB per hour) or (None, None) if not found
	"""
	try:
		#solve B301
		parsed_url = urllib.parse.urlparse(FARGATE_PRICING_URL)
		if parsed_url.scheme not in ('http', 'https'):
			raise ValueError("Only HTTP and HTTPS URLs are allowed for Fargate pricing data.")
		
		# Fetch the pricing data
		with urllib.request.urlopen(FARGATE_PRICING_URL) as response:
			pricing_data = json.loads(response.read().decode())

		# Find ARM vCPU and memory prices for the specified region
		vcpu_price = None
		memory_price = None

		for price in pricing_data.get('prices', []):
			if price.get('attributes', {}).get('aws:region') == region:
				if price.get('unit') == 'ARM-vCPU-Hours':
					vcpu_price = Decimal(price.get('price', {}).get('USD', '0'))
				elif price.get('unit') == 'ARM-GB-Hours':
					memory_price = Decimal(price.get('price', {}).get('USD', '0'))

		if not vcpu_price or not memory_price:
			logger.warning(f'Could not find Fargate Spot pricing for region {region}')
			return None, None

		return vcpu_price, memory_price

	except Exception as e:
		logger.error(f'Error fetching Fargate pricing: {str(e)}')
		return None, None


def get_fargate_ephemeral_storage_price(region: str) -> Optional[Decimal]:
	"""
	Get the price for Fargate ephemeral storage beyond the free tier (20GB) using the AWS Pricing API.

	Args:
	    region: AWS region code (e.g., 'us-east-1')

	Returns:
	    Decimal: Cost per GB-Hour for ephemeral storage beyond 20GB, or None if not found
	"""
	try:
		# Initialize the pricing client in us-east-1 (the only region endpoint for pricing API)
		pricing_client = boto3.client('pricing', region_name='us-east-1')

		# Query for Fargate ephemeral storage pricing using the verified filters
		response = pricing_client.get_products(
			ServiceCode='AmazonECS',
			Filters=[
				{'Type': 'TERM_MATCH', 'Field': 'regionCode', 'Value': region},
				{'Type': 'TERM_MATCH', 'Field': 'storagetype', 'Value': 'default'},
			],
			MaxResults=100,
		)

		# Parse the pricing data
		for price_item_str in response.get('PriceList', []):
			price_item = json.loads(price_item_str)

			# Based on the CLI response, make sure we're looking at ephemeral storage
			attributes = price_item.get('product', {}).get('attributes', {})
			usage_type = attributes.get('usagetype', '')

			# Check if this is Fargate Ephemeral Storage
			if 'Fargate-EphemeralStorage-GB-Hours' in usage_type:
				# Extract the price from the price list
				terms = price_item.get('terms', {})
				on_demand = terms.get('OnDemand', {})

				# Extract the first pricing dimension we find
				for dimension_key in on_demand:
					price_dimensions = on_demand[dimension_key].get('priceDimensions', {})
					for price_dimension in price_dimensions.values():
						if 'pricePerUnit' in price_dimension:
							usd_price = price_dimension['pricePerUnit'].get('USD')
							if usd_price:
								return Decimal(usd_price)

		logger.warning(f'No ephemeral storage pricing found for region {region}')
		return None

	except Exception as e:
		logger.exception(f'Error retrieving ephemeral storage cost: {str(e)}')
		return None


def calculate_fargate_cost_per_minute(region: str, cpu: int, memory_mb: int, ephemeral_storage_gb: int = 20) -> Decimal:
	"""
	Calculate the cost of running Fargate per minute for the given resources.

	Args:
	    region: AWS region
	    cpu: CPU units (1024 = 1 vCPU)
	    memory_mb: Memory in MBs
	    ephemeral_storage_gb: Ephemeral storage in GB (default 20)

	Returns:
	    Decimal: Cost per minute in USD
	"""
	# Get Fargate spot pricing for CPU and memory
	vcpu_price_per_hour, memory_price_per_hour = get_fargate_spot_pricing(region)

	if not vcpu_price_per_hour or not memory_price_per_hour:
		logger.warning(f'Could not find CPU/memory pricing for region {region}')
		return Decimal('0')

	# Convert CPU units to vCPU (1024 = 1 vCPU)
	vcpu_count = Decimal(cpu) / Decimal('1024')
	memory_gb = Decimal(memory_mb) / Decimal('1024')

	# Calculate compute and memory costs
	compute_memory_cost = (vcpu_count * vcpu_price_per_hour) + (Decimal(memory_gb) * memory_price_per_hour)

	# Calculate ephemeral storage cost (only for the portion above 20GB)
	storage_cost = Decimal('0')
	ephemeral_storage_gb = Decimal(ephemeral_storage_gb)
	storage_gb_over_default = max(0, ephemeral_storage_gb - 20)

	if storage_gb_over_default > 0:
		storage_price_per_gb_hour = get_fargate_ephemeral_storage_price(region)
		if storage_price_per_gb_hour:
			storage_cost = storage_gb_over_default * storage_price_per_gb_hour
			logger.info(
				f'Ephemeral storage cost: ${storage_cost} per hour for {storage_gb_over_default}GB above default'
			)

	# Calculate total hourly cost
	hourly_cost = compute_memory_cost + storage_cost

	# Convert to per minute cost
	per_minute_cost = hourly_cost / Decimal('60')

	return per_minute_cost


def _get_data_transfer_cost(source_region: str, destination_region: str) -> Optional[Decimal]:
	"""
	Retrieve the cost per GB for data transfer from source_region to destination_region.

	Args:
	    source_region: The AWS source region code (e.g., 'us-east-1')
	    destination_region: The AWS destination region code (e.g., 'eu-west-1')

	Returns:
	    Decimal: Cost per GB in USD, or None if pricing information could not be found
	"""
	try:
		# Initialize the pricing client in us-east-1 (the only region endpoint for pricing API)
		session = boto3.Session()
		pricing_client = session.client('pricing', region_name='us-east-1')

		# Query for data transfer pricing
		response = pricing_client.get_products(
			ServiceCode='AWSDataTransfer',
			Filters=[
				{'Type': 'TERM_MATCH', 'Field': 'fromRegionCode', 'Value': source_region},
				{'Type': 'TERM_MATCH', 'Field': 'toRegionCode', 'Value': destination_region},
				{'Type': 'TERM_MATCH', 'Field': 'transferType', 'Value': 'InterRegion Outbound'},
			],
			MaxResults=100,
		)

		# Parse the pricing data
		cost = _parse_pricing_data(response)
		if cost is not None:
			logger.info(f'Data transfer cost from {source_region} to {destination_region}: ${cost} per GB')
			return cost

		logger.warning(f'No pricing data found for transfer from {source_region} to {destination_region}')
		return None

	except Exception as e:
		logger.exception(f'Error retrieving data transfer cost: {str(e)}')
		return None


def _parse_pricing_data(response: dict) -> Optional[Decimal]:
	"""
	Parse the AWS pricing API response to extract the cost per GB.

	Args:
	    response: AWS Pricing API response

	Returns:
	    Decimal: Cost per GB in USD, or None if parsing fails
	"""
	try:
		if 'PriceList' not in response or not response['PriceList']:
			return None

		# The pricing data is returned as a JSON string within the response
		for price_item_str in response['PriceList']:
			import json

			price_item = json.loads(price_item_str)

			terms = price_item.get('terms', {})
			on_demand = terms.get('OnDemand', {})

			# Extract the first pricing dimension we find
			for dimension_key in on_demand:
				price_dimensions = on_demand[dimension_key].get('priceDimensions', {})
				for price_dimension in price_dimensions.values():
					if 'pricePerUnit' in price_dimension:
						usd_price = price_dimension['pricePerUnit'].get('USD')
						if usd_price:
							return Decimal(usd_price)

		return None

	except Exception as e:
		logger.exception(f'Error parsing pricing data: {str(e)}')
		return None


def get_average_data_transfer_cost(
	source_region: str,
	destination_regions: List[str],
) -> Optional[Decimal]:
	"""
	Retrieve the average cost per GB for data transfer from source_region to multiple destination_regions.

	Args:
	    source_region: The AWS source region code (e.g., 'us-east-1')
	    destination_regions: List of AWS destination region codes

	Returns:
	    Decimal: Average cost per GB in USD across all destination regions,
	             or None if pricing information could not be found for any region
	"""
	if not destination_regions:
		logger.warning('No destination regions provided')
		return None

	costs = []

	for destination_region in destination_regions:
		cost = _get_data_transfer_cost(source_region, destination_region)
		if cost is not None:
			costs.append(cost)

	if not costs:
		logger.warning(f'No valid pricing data found for transfer from {source_region} to any of the provided regions')
		return None

	# Calculate the average
	average_cost = sum(costs) / len(costs)
	logger.info(f'Average data transfer cost from {source_region} to {len(costs)} regions: ${average_cost} per GB')

	return average_cost


def send_cfn_response(
	event: Dict[str, Any], context: Any, status: str, response_data: Dict[str, Any], physical_id: str = None
) -> None:
	"""
	Send a response to CloudFormation regarding the status of the resource.

	Args:
	    event: Lambda event
	    context: Lambda context
	    status: SUCCESS or FAILED
	    response_data: Data to send back to CloudFormation
	    physical_id: Physical ID of the resource
	"""
	response_body = {
		'Status': status,
		'Reason': f'See CloudWatch Log Stream: {context.log_stream_name}',
		'PhysicalResourceId': physical_id or context.log_stream_name,
		'StackId': event.get('StackId'),
		'RequestId': event.get('RequestId'),
		'LogicalResourceId': event.get('LogicalResourceId'),
		'Data': response_data,
	}

	logger.info(f'Sending response: {json.dumps(response_body)}')

	# Convert Decimal objects to strings for JSON serialization
	response_json = json.dumps(response_body, default=lambda x: str(x) if isinstance(x, Decimal) else x)

	# Send the response back to CloudFormation
	response_url = event.get('ResponseURL')

	if not response_url:
		logger.warning('No ResponseURL found in event')
		return

	headers = {'Content-Type': '', 'Content-Length': str(len(response_json))}

	parsed_url = urllib.parse.urlparse(response_url)
	if parsed_url.scheme not in ('http', 'https'):
		logger.error(f"Refusing to send response to non-HTTP(S) URL: {response_url}")
		return

	req = urllib.request.Request(url=response_url, data=response_json.encode('utf-8'), headers=headers, method='PUT')

	try:
		with urllib.request.urlopen(req) as response:
			logger.info(f'Status code: {response.getcode()}')
			logger.info(f'Status message: {response.msg}')
	except Exception as e:
		logger.error(f'Error sending response to CloudFormation: {str(e)}')


def lambda_handler(event: Dict[str, Any], context: Any) -> None:
	"""
	Lambda handler function for the CloudFormation Custom Resource.

	Args:
	    event: Lambda event data
	    context: Lambda context
	"""
	logger.info(f'Received event: {json.dumps(event, default=str)}')

	# Initialize response data
	response_data = {}

	try:
		# Extract request type from CloudFormation event
		request_type = event.get('RequestType')

		if request_type == 'Delete':
			# For Delete events, just return success
			send_cfn_response(event, context, 'SUCCESS', response_data)
			return

		# Extract parameters from the event
		logger.info(f'Extracting parameters from the event: {event}')
		resource_property = event.get('ResourceProperties', {})
		region = resource_property.get('AwsRegion')
		cpu = resource_property.get('FargateCpu')
		memory = resource_property.get('FargateMemory')
		ephemeral_disk = resource_property.get('FargateEphemeralDisk', '20')  # Default to 20GB if not specified
		target_regions_str = resource_property.get('TargetRegions', '')

		# Parse the target regions list
		target_regions = (
			[r.strip() for r in target_regions_str.split(',')]
			if isinstance(target_regions_str, str)
			else target_regions_str
		)

		# Validate required parameters
		if not all([region, cpu, memory]):
			error_msg = 'Missing required parameters. Required: AwsRegion, FargateCpu, FargateMemory'
			logger.error(error_msg)
			send_cfn_response(event, context, 'FAILED', {'Error': error_msg})
			return

		# Calculate Fargate costs
		fargate_cost_per_minute = calculate_fargate_cost_per_minute(region, int(cpu), int(memory), int(ephemeral_disk))

		# Get average data transfer cost if target regions were provided
		avg_data_transfer_cost = None
		if target_regions:
			avg_data_transfer_cost = get_average_data_transfer_cost(region, target_regions)

		# Prepare response with results
		response_data = {
			'FargateCostPerMinute': str(fargate_cost_per_minute),
			'AverageDataTransferCostPerGB': str(avg_data_transfer_cost) if avg_data_transfer_cost else '0',
			'Region': region,
			'TargetRegions': ','.join(target_regions) if isinstance(target_regions, list) else target_regions,
			'EphemeralDiskGB': ephemeral_disk,
			'CPU': cpu,
			'MemoryGB': memory,
		}

		logger.info(f'Calculation results: {json.dumps(response_data, default=str)}')
		send_cfn_response(event, context, 'SUCCESS', response_data)

	except Exception as e:
		logger.exception(f'Error processing request: {str(e)}')
		send_cfn_response(event, context, 'FAILED', {'Error': str(e)})
