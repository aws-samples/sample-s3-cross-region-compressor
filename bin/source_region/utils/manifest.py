"""
Manifest Utilities for Source Region Container

This module provides utilities for creating and managing manifest files:
- Generate manifest JSON structure
- Add object metadata
- Add target information
"""

import json
import logging
from typing import Dict, List, Optional

# Configure logging
logger = logging.getLogger(__name__)


def create_manifest_structure() -> Dict:
	"""
	Create the basic structure for a manifest file.

	Returns:
	    Dictionary with the manifest structure
	"""
	return {'targets': [], 'objects': []}


def add_object_to_manifest(manifest: Dict, object_metadata: Dict, targets: List[Dict] = None) -> Dict:
	"""
	Add an object to the manifest with its metadata.

	Args:
	    manifest: Manifest dictionary
	    object_metadata: Object metadata dictionary
	    targets: List of target dictionaries (not used, kept for compatibility)

	Returns:
	    Updated manifest dictionary
	"""
	# Create object entry
	object_entry = {
		'source_bucket': object_metadata.get('source_bucket', ''),
		'source_prefix': object_metadata.get('source_prefix', ''),
		'object_name': object_metadata.get('object_name', ''),
		# Add relative_key for preserving directory structure and avoiding name collisions
		'relative_key': object_metadata.get('relative_key', object_metadata.get('object_name', '')),
		'tags': object_metadata.get('tags', []),
		'creation_time': object_metadata.get('creation_time', ''),
		'etag': object_metadata.get('etag', ''),
		'size': object_metadata.get('size', 0),
		'storage_class': object_metadata.get('storage_class', 'STANDARD'),
	}

	# Add to manifest
	manifest['objects'].append(object_entry)

	return manifest


def write_manifest_to_file(manifest: Dict, output_path: str) -> bool:
	"""
	Write a manifest dictionary to a JSON file.

	Args:
	    manifest: Manifest dictionary
	    output_path: Path to output file

	Returns:
	    True if successful, False otherwise
	"""
	try:
		with open(output_path, 'w') as f:
			json.dump(manifest, f, indent=4)
		return True
	except Exception as e:
		logger.error(f'Error writing manifest to file: {e}')
		return False


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


def create_object_manifest(objects_metadata: List[Dict], targets: List[Dict], output_path: str) -> bool:
	"""
	Create a complete manifest file for multiple objects.

	Args:
	    objects_metadata: List of object metadata dictionaries
	    output_path: Path to output file

	Returns:
	    True if successful, False otherwise
	"""
	try:
		# Create manifest structure
		manifest = create_manifest_structure()

		# Set targets at the top level (targets now contain backup flags)
		manifest['targets'] = targets

		# Add each object
		for obj_metadata in objects_metadata:
			add_object_to_manifest(manifest, obj_metadata)

		# Write to file
		return write_manifest_to_file(manifest, output_path)
	except Exception as e:
		logger.error(f'Error creating object manifest: {e}')
		return False
