import json
from typing import Dict, List, Any


def get_config(json_dir):
	"""
	Load a JSON configuration file.

	Args:
	    json_dir: Path to the JSON file

	Returns:
	    The loaded JSON configuration as a Python object
	"""
	with open(json_dir, 'r') as json_file:
		config = json.load(json_file)
		return config


def detect_replication_loops(config):
	"""
	Detect if there are any replication loops in the configuration.

	Args:
	    config: The replication configuration object

	Returns:
	    True if a replication loop is detected, False otherwise
	    
	Notes:
	    This function detects replication loops considering both bucket names and prefixes.
	    A loop exists only if objects could theoretically cycle through the same paths.
	"""
	# Extract all replication rules
	rules = config.get('replication_configuration', [])
	
	# Create a more precise graph representation that includes prefixes
	# The graph will be a dictionary of dictionaries:
	# { (bucket, prefix): { (dest_bucket, dest_prefix): True } }
	graph = {}
	
	# Store rules by bucket and prefix
	bucket_rules = {}
	
	# Build the graph
	for rule in rules:
		source_bucket = rule['source']['bucket']
		source_prefix = rule['source'].get('prefix_filter', '')
		
		# Normalize empty prefixes to avoid None issues
		if source_prefix is None:
			source_prefix = ""
			
		source_key = (source_bucket, source_prefix)
		
		if source_key not in graph:
			graph[source_key] = {}
			
		# Track all rules for each bucket to help with prefix analysis
		if source_bucket not in bucket_rules:
			bucket_rules[source_bucket] = []
		bucket_rules[source_bucket].append(source_prefix)
		
		# Add edges from source to all destinations
		for dest in rule['destinations']:
			dest_bucket = dest['bucket']
			# Create a connection in the graph
			graph[source_key][dest_bucket] = True
			
	# Helper function to check if two buckets with specific prefixes form a loop
	def forms_loop(bucket_a, prefix_a, bucket_b, prefix_b_param):
		"""
		Check if two bucket+prefix configurations form a replication loop.
		A loop exists if:
		1. bucket_a (with prefix_a) replicates to bucket_b
		2. bucket_b (with prefix_b) replicates to bucket_a
		   AND the prefix_b could affect objects under prefix_a
		"""
		# Check if bucket_a (with prefix_a) replicates to bucket_b
		source_key_a = (bucket_a, prefix_a)
		if source_key_a not in graph or bucket_b not in graph[source_key_a]:
			return False
			
		# Check if bucket_b (with prefix_b) replicates to bucket_a
		source_key_b = (bucket_b, prefix_b_param)
		if source_key_b not in graph or bucket_a not in graph[source_key_b]:
			return False
		
		# There are several cases to consider:
		# 1. If both prefixes are the same - that's a clear loop
		if prefix_a == prefix_b_param:
			return True
			
		# 2. If both prefixes are non-empty and different - no loop
		# (Different folders in the buckets)
		if prefix_a and prefix_b_param and prefix_a != prefix_b_param:
			return False
			
		# 3. If one prefix is empty - this is a loop!
		# When a prefix is empty, it means "replicate the entire bucket"
		# So any object could potentially cycle through
		if not prefix_a or not prefix_b_param:
			return True
			
		# If we get here (which we shouldn't), assume it's a loop scenario
		return True
	
	# Check all possible pairs of bucket rules for loops
	for bucket_a, prefixes_a in bucket_rules.items():
		for prefix_a in prefixes_a:
			for bucket_b, prefixes_b in bucket_rules.items():
				for prefix_b in prefixes_b:
					# Skip self-comparisons for the same bucket+prefix
					if bucket_a == bucket_b and prefix_a == prefix_b:
						continue
						
					if forms_loop(bucket_a, prefix_a, bucket_b, prefix_b):
						return True
						
	return False


def group_configurations_by_source_region(config_or_path: Any) -> Dict[str, List[Dict]]:
	"""
	Group replication configurations by source region.

	Args:
	    config_or_path: Either a path to the replication_config.json file
	                   or the already loaded configuration object

	Returns:
	    A dictionary with source regions as keys, and lists of source bucket
	    configurations with their destination regions as values
	"""
	# Load the configuration if a string path is provided
	if isinstance(config_or_path, str):
		config = get_config(config_or_path)
	else:
		config = config_or_path

	# Get the replication configuration array
	if isinstance(config, dict) and 'replication_configuration' in config:
		replication_config = config['replication_configuration']
	else:
		# Assume the config is already the replication_configuration array
		replication_config = config

	# Initialize the result dictionary
	result = {}

	# Process each replication configuration
	for item in replication_config:
		source_region = item['source']['region']
		source_bucket = item['source']['bucket']
		source_prefix = item['source'].get('prefix_filter', '')
		destination_regions = [dest['region'] for dest in item['destinations']]

		# Add to the appropriate source region group
		if source_region not in result:
			result[source_region] = []

		# Create the source bucket configuration with its destinations
		source_config = {
			'source_bucket': source_bucket,
			'source_prefix': source_prefix,
			'destinations': destination_regions,
		}

		result[source_region].append(source_config)

	return result
