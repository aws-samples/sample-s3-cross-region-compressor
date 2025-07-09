"""
Manifest Utilities for Target Region Container

This module provides utilities for parsing and processing manifest files:
- Read manifest JSON structure
- Extract object metadata and target information
"""

import json
import logging
import os
from typing import Dict, List, Optional

# Configure logging
logger = logging.getLogger(__name__)


def read_manifest_from_file(file_path: str) -> Optional[Dict]:
	"""
	Read a manifest from a JSON file.

	Args:
	    file_path: Path to manifest file

	Returns:
	    Manifest dictionary or None if error
	"""
	try:
		with open(file_path, 'r') as f:
			return json.load(f)
	except Exception as e:
		logger.error(f'Error reading manifest from file: {e}')
		return None


def get_object_paths_from_manifest(manifest: Dict, extract_dir: str) -> List[Dict]:
	"""
	Get paths to objects from the manifest.

	Note: This function no longer checks for file existence to support streaming extraction,
	where files will be extracted one by one later in the process.

	Args:
	    manifest: Manifest dictionary
	    extract_dir: Directory where files will be extracted

	Returns:
	    List of dictionaries with object information
	"""
	object_paths = []

	# Check if manifest has objects
	objects = manifest.get('objects', [])
	if not objects:
		logger.warning('No objects found in manifest')
		return []

	# Process each object
	for obj in objects:
		object_name = obj.get('object_name')
		if not object_name:
			logger.warning('Object missing name in manifest')
			continue

		# Get relative key (path) if available, fall back to object_name
		relative_key = obj.get('relative_key', object_name)

		# Construct expected path to object using relative key (will be extracted later)
		object_path = os.path.join(extract_dir, 'objects', relative_key)

		# Add object information without checking file existence
		object_info = {
			'local_path': object_path,  # This will be the path after extraction
			'object_name': object_name,
			'relative_key': relative_key,
			'source_bucket': obj.get('source_bucket', ''),
			'source_prefix': obj.get('source_prefix', ''),
			'tags': obj.get('tags', []),
			'creation_time': obj.get('creation_time', ''),
			'etag': obj.get('etag', ''),
			'size': obj.get('size', 0),
			'storage_class': obj.get('storage_class', 'STANDARD'),
		}

		# Get targets from the top level of the manifest
		if 'targets' in manifest:
			object_info['targets'] = manifest.get('targets', [])
		else:
			logger.warning(f'No targets found in manifest for object: {object_name}')
			continue

		# Log target information for debugging
		for target in object_info.get('targets', []):
			if 'storage_class' in target:
				logger.debug(
					f"Found storage_class '{target['storage_class']}' in target config for region {target.get('region', 'unknown')}"
				)

		object_paths.append(object_info)

	return object_paths


def prepare_object_tags(object_info: Dict) -> Dict[str, str]:
	"""
	Prepare tags for an object, including original creation time and etag.

	Args:
	    object_info: Object information dictionary

	Returns:
	    Dictionary of tags
	"""
	# Start with an empty tags dictionary
	tags = {}

	# Process existing tags
	for tag_dict in object_info.get('tags', []):
		for key, value in tag_dict.items():
			tags[key] = value

	# Add original creation time and etag tags
	if object_info.get('creation_time'):
		tags['OriginalCreationTime'] = object_info['creation_time']

	if object_info.get('etag'):
		tags['OriginalETag'] = object_info['etag']

	return tags
