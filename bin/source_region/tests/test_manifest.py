"""
Unit tests for the manifest module.
"""
import tempfile
#added for B108 vulnerability resolution

import json
import os

# Import the module under test
from bin.source_region.utils.manifest import (
	create_manifest_structure,
	add_object_to_manifest,
	write_manifest_to_file,
	read_manifest_from_file,
	create_object_manifest,
)


class TestManifestStructure:
	"""Tests for manifest structure creation and manipulation."""

	def test_create_manifest_structure(self):
		"""Test creating the basic manifest structure."""
		# When: We create a manifest structure
		manifest = create_manifest_structure()

		# Then: It should have the expected structure
		assert isinstance(manifest, dict)
		assert 'targets' in manifest
		assert 'objects' in manifest
		assert isinstance(manifest['targets'], list)
		assert isinstance(manifest['objects'], list)
		assert len(manifest['targets']) == 0
		assert len(manifest['objects']) == 0

	def test_add_object_to_manifest(self):
		"""Test adding an object to a manifest."""
		# Given: A manifest structure and object metadata
		manifest = create_manifest_structure()
		object_metadata = {
			'source_bucket': 'test-bucket',
			'source_prefix': 'test/prefix',
			'object_name': 'test-object.txt',
			'relative_key': 'prefix/test-object.txt',
			'tags': [{'Purpose': 'Testing'}],
			'creation_time': '2023-01-01 00:00:00',
			'etag': 'abc123',
			'size': 1024,
			'storage_class': 'STANDARD',
		}

		# When: We add the object to the manifest
		updated_manifest = add_object_to_manifest(manifest, object_metadata)

		# Then: The manifest should contain the object with all its metadata
		assert len(updated_manifest['objects']) == 1
		added_object = updated_manifest['objects'][0]
		assert added_object['source_bucket'] == 'test-bucket'
		assert added_object['source_prefix'] == 'test/prefix'
		assert added_object['object_name'] == 'test-object.txt'
		assert added_object['relative_key'] == 'prefix/test-object.txt'
		assert added_object['tags'] == [{'Purpose': 'Testing'}]
		assert added_object['creation_time'] == '2023-01-01 00:00:00'
		assert added_object['etag'] == 'abc123'
		assert added_object['size'] == 1024
		assert added_object['storage_class'] == 'STANDARD'

	def test_add_object_to_manifest_minimal(self):
		"""Test adding an object with minimal metadata to a manifest."""
		# Given: A manifest structure and minimal object metadata
		manifest = create_manifest_structure()
		object_metadata = {'source_bucket': 'test-bucket', 'object_name': 'test-object.txt'}

		# When: We add the object to the manifest
		updated_manifest = add_object_to_manifest(manifest, object_metadata)

		# Then: The manifest should contain the object with default values for missing metadata
		assert len(updated_manifest['objects']) == 1
		added_object = updated_manifest['objects'][0]
		assert added_object['source_bucket'] == 'test-bucket'
		assert added_object['source_prefix'] == ''
		assert added_object['object_name'] == 'test-object.txt'
		assert added_object['relative_key'] == 'test-object.txt'
		assert added_object['tags'] == []
		assert added_object['creation_time'] == ''
		assert added_object['etag'] == ''
		assert added_object['size'] == 0
		assert added_object['storage_class'] == 'STANDARD'

	def test_add_object_with_relative_key(self):
		"""Test adding an object with a relative key to a manifest."""
		# Given: A manifest structure and object metadata with relative_key
		manifest = create_manifest_structure()
		object_metadata = {
			'source_bucket': 'test-bucket',
			'source_prefix': 'test',
			'object_name': 'object.txt',
			'relative_key': 'test/nested/object.txt',
		}

		# When: We add the object to the manifest
		updated_manifest = add_object_to_manifest(manifest, object_metadata)

		# Then: The manifest should preserve the relative key
		assert len(updated_manifest['objects']) == 1
		added_object = updated_manifest['objects'][0]
		assert added_object['relative_key'] == 'test/nested/object.txt'


class TestManifestIO:
	"""Tests for manifest file I/O operations."""

	def test_write_manifest_to_file(self, temp_directory):
		"""Test writing a manifest to a file."""
		# Given: A manifest structure
		manifest = {
			'targets': [{'region': 'us-west-2', 'bucket': 'target-bucket', 'storage_class': 'STANDARD'}],
			'objects': [
				{
					'source_bucket': 'test-bucket',
					'source_prefix': 'test',
					'object_name': 'test-object.txt',
					'relative_key': 'test/test-object.txt',
					'size': 1024,
				}
			],
		}
		output_path = os.path.join(temp_directory, 'manifest.json')

		# When: We write the manifest to a file
		result = write_manifest_to_file(manifest, output_path)

		# Then: The write should be successful and the file should contain the correct content
		assert result is True
		assert os.path.exists(output_path)

		with open(output_path, 'r') as f:
			loaded_manifest = json.load(f)
			assert loaded_manifest == manifest

	def test_write_manifest_error(self, temp_directory):
		"""Test handling errors when writing a manifest."""
		# Given: A manifest structure and an invalid path
		manifest = create_manifest_structure()
		invalid_path = os.path.join(temp_directory, 'nonexistent_dir', 'manifest.json')

		# When: We try to write to an invalid path
		result = write_manifest_to_file(manifest, invalid_path)

		# Then: The write should fail
		assert result is False
		assert not os.path.exists(invalid_path)

	def test_read_manifest_from_file(self, temp_directory):
		"""Test reading a manifest from a file."""
		# Given: A manifest file
		manifest = {
			'targets': [{'region': 'us-west-2', 'bucket': 'target-bucket', 'storage_class': 'STANDARD'}],
			'objects': [
				{
					'source_bucket': 'test-bucket',
					'source_prefix': 'test',
					'object_name': 'test-object.txt',
					'size': 1024,
				}
			],
		}
		file_path = os.path.join(temp_directory, 'manifest.json')
		with open(file_path, 'w') as f:
			json.dump(manifest, f)

		# When: We read the manifest from the file
		loaded_manifest = read_manifest_from_file(file_path)

		# Then: The loaded manifest should match the original
		assert loaded_manifest == manifest

	def test_read_manifest_nonexistent_file(self):
		"""Test reading a nonexistent manifest file."""
		# Given: A nonexistent file path
		#nonexistent_path = '/tmp/nonexistent_manifest.json'
		#Solving B108
		with tempfile.TemporaryDirectory() as temp_directory:
			nonexistent_path = os.path.join(temp_directory, 'nonexistent_manifest.json')
		# When: We try to read the nonexistent file
			result = read_manifest_from_file(nonexistent_path)

		# Then: The result should be None due to error handling
			assert result is None

	def test_read_manifest_invalid_json(self, temp_directory):
		"""Test reading an invalid JSON file as a manifest."""
		# Given: A file with invalid JSON
		invalid_json_path = os.path.join(temp_directory, 'invalid.json')
		with open(invalid_json_path, 'w') as f:
			f.write('This is not valid JSON')

		# When: We try to read the invalid JSON file
		result = read_manifest_from_file(invalid_json_path)

		# Then: The result should be None due to error handling
		assert result is None


class TestCompleteManifestCreation:
	"""Tests for the complete manifest creation workflow."""

	def test_create_object_manifest(self, temp_directory):
		"""Test creating a complete object manifest."""
		# Given: Object metadata and targets
		objects_metadata = [
			{
				'source_bucket': 'test-bucket',
				'source_prefix': 'test/prefix1',
				'object_name': 'object1.txt',
				'relative_key': 'test/prefix1/object1.txt',
				'tags': [{'Purpose': 'Testing'}],
				'creation_time': '2023-01-01 00:00:00',
				'etag': 'abc123',
				'size': 1024,
				'storage_class': 'STANDARD',
			},
			{
				'source_bucket': 'test-bucket',
				'source_prefix': 'test/prefix2',
				'object_name': 'object2.txt',
				'relative_key': 'test/prefix2/object2.txt',
				'tags': [{'Environment': 'Dev'}],
				'creation_time': '2023-01-01 00:00:00',
				'etag': 'def456',
				'size': 2048,
				'storage_class': 'STANDARD',
			},
		]

		targets = [
			{'region': 'us-west-2', 'bucket': 'target-bucket-west', 'storage_class': 'STANDARD'},
			{'region': 'eu-west-1', 'bucket': 'target-bucket-eu', 'storage_class': 'STANDARD_IA'},
		]

		output_path = os.path.join(temp_directory, 'complete_manifest.json')

		# When: We create a complete object manifest
		result = create_object_manifest(objects_metadata, targets, output_path)

		# Then: The creation should be successful and the file should contain the correct content
		assert result is True
		assert os.path.exists(output_path)

		# Verify the content
		with open(output_path, 'r') as f:
			manifest = json.load(f)

			# Check targets
			assert len(manifest['targets']) == 2
			assert manifest['targets'][0]['region'] == 'us-west-2'
			assert manifest['targets'][1]['bucket'] == 'target-bucket-eu'

			# Check objects
			assert len(manifest['objects']) == 2
			assert manifest['objects'][0]['object_name'] == 'object1.txt'
			assert manifest['objects'][1]['relative_key'] == 'test/prefix2/object2.txt'

	def test_create_object_manifest_error(self, temp_directory):
		"""Test handling errors during manifest creation."""
		# Given: Invalid inputs
		objects_metadata = None
		targets = [{'region': 'us-west-2', 'bucket': 'target-bucket'}]
		output_path = os.path.join(temp_directory, 'should_fail.json')

		# When: We try to create a manifest with invalid inputs
		result = create_object_manifest(objects_metadata, targets, output_path)

		# Then: The creation should fail
		assert result is False
		assert not os.path.exists(output_path)
