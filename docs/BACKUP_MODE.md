# Backup Mode for S3 Cross-Region Compressor

This guide provides comprehensive information on using the backup mode feature of the S3 Cross-Region Compressor system, which allows you to store compressed archives instead of individual decompressed files in target regions.

## Overview

Backup mode is a destination-level configuration that changes how the system handles replicated data. Instead of decompressing and storing individual files, the system stores the entire compressed archive (`.tar.zst` file) along with a searchable metadata catalog.

### Key Benefits

- **Storage Efficiency**: Compressed archives use significantly less storage space
- **Cost Optimization**: Reduced storage costs, especially for infrequently accessed data
- **Batch Integrity**: Maintains the original compression batches for audit purposes
- **Searchable Catalog**: Query backup contents using AWS Athena without decompressing
- **Flexible Recovery**: Decompress only the files you need when required

## Configuration

### Enabling Backup Mode

Backup mode is configured per destination in the `replication_config.json` file by adding the `backup` flag:

```json
{
    "replication_configuration": [
        {
            "source": {
                "region": "us-west-2",
                "bucket": "source-bucket",
                "prefix_filter": "documents/"
            },
            "destinations": [
                {
                    "region": "eu-central-1",
                    "bucket": "target-bucket-backup",
                    "storage_class": "STANDARD",
                    "backup": true
                },
                {
                    "region": "ca-central-1", 
                    "bucket": "target-bucket-normal",
                    "storage_class": "STANDARD"
                }
            ]
        }
    ]
}
```

### Mixed Mode Configuration

You can configure different destinations with different modes for the same source:

- **Backup destinations** (`backup: true`): Receive compressed archives
- **Normal destinations** (no backup flag or `backup: false`): Receive individual decompressed files

This allows you to have both immediate access (normal mode) and cost-effective archival (backup mode) simultaneously.

### Configuration Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `backup` | Boolean | No | Set to `true` to enable backup mode for this destination (default: `false`) |

## How Backup Mode Works

### Source Region Processing

1. **Object Detection**: Source region detects new S3 objects via SQS notifications
2. **Compression**: Objects are compressed together into `.tar.zst` archives
3. **Manifest Creation**: A manifest file is created containing metadata for all objects
4. **Staging Upload**: Compressed archive is uploaded to staging bucket
5. **Cross-Region Replication**: S3 replication transfers archive to target regions

### Target Region Processing

1. **Archive Reception**: Target region receives compressed archive from staging bucket
2. **Manifest Extraction**: System extracts and reads the manifest file
3. **Destination Routing**: 
   - **Backup destinations**: Receive the compressed archive as-is
   - **Normal destinations**: Archive is decompressed and individual files are extracted
4. **Catalog Creation**: For backup destinations, metadata is written to searchable catalog

### File Structure in Backup Destinations

Backup destinations receive files in the following structure:

```
target-bucket/
├── source-prefix/
│   ├── backup_1753329108085.tar.zst    # Compressed archive
│   ├── backup_1753329108659.tar.zst    # Another compressed archive
│   └── backup_1753329109343.tar.zst    # Yet another compressed archive
```

Each `.tar.zst` file contains:
- Multiple source objects compressed together
- A `manifest.json` file with metadata for all objects
- Preserved directory structure from source

## Catalog and Search Functionality

### Automatic Catalog Creation

When backup mode is enabled, the system automatically creates:

1. **S3 Catalog Bucket**: `{stack-name}-{account}-{region}-catalog`
2. **Glue Database**: `{stack-name}_catalog_db`
3. **Glue Crawler**: Scans catalog daily to update schema
4. **Athena Workgroup**: `{stack-name}-catalog-workgroup` for queries

### Catalog Metadata Structure

The catalog stores metadata in JSONL format with the following fields:

| Field | Type | Description |
|-------|------|-------------|
| `backup_file` | String | Name of the compressed archive file |
| `backup_timestamp` | Number | Unix timestamp when backup was created |
| `backup_date` | String | Date in YYYY-MM-DD format |
| `source_bucket` | String | Original source bucket name |
| `source_prefix` | String | Source prefix/folder path |
| `object_name` | String | Individual file name |
| `object_path` | String | Complete path (prefix + object name) |
| `object_size` | Number | File size in bytes |
| `creation_time` | String | When the original file was created |
| `creation_date` | String | Creation date in YYYY-MM-DD format |
| `target_buckets` | Array | List of backup destination buckets |

### Catalog File Organization

Catalog files are organized with date partitioning for efficient querying:

```
catalog-bucket/
├── source-bucket/
│   └── source-prefix/
│       ├── year=2025/
│       │   └── month=01/
│       │       └── day=23/
│       │           ├── backup_1753329108085.tar.zst.jsonl
│       │           ├── backup_1753329108659.tar.zst.jsonl
│       │           └── backup_1753329109343.tar.zst.jsonl
│       └── year=2025/
│           └── month=01/
│               └── day=24/
│                   └── backup_1753329200123.tar.zst.jsonl
```

## Querying Backup Contents

### Using AWS Athena

Access the Athena console and select the workgroup created by the system:

1. **Navigate to Athena**: AWS Console → Athena
2. **Select Workgroup**: Choose `{stack-name}-catalog-workgroup`
3. **Select Database**: Use `{stack-name}_catalog_db`
4. **Run Queries**: Query the catalog table

### Example Queries

#### Find All Files in a Specific Backup

```sql
SELECT object_name, object_size, creation_date 
FROM catalog_table_name
WHERE backup_file = 'backup_1753329108085.tar.zst'
ORDER BY object_name;
```

#### Search for Files by Name Pattern

```sql
SELECT backup_file, object_name, object_path, backup_date
FROM catalog_table_name 
WHERE object_name LIKE '%invoice%'
ORDER BY backup_date DESC;
```

#### Find Files by Date Range

```sql
SELECT backup_file, object_name, object_size
FROM catalog_table_name
WHERE year = '2025' 
  AND month = '01' 
  AND day BETWEEN '20' AND '25'
ORDER BY backup_timestamp DESC;
```

#### Find Which Backup Contains a Specific File

```sql
SELECT backup_file, source_bucket, source_prefix, backup_date
FROM catalog_table_name
WHERE object_name = 'important-document.pdf'
ORDER BY backup_date DESC
LIMIT 1;
```

#### Get Backup Statistics by Date

```sql
SELECT backup_date, 
       COUNT(*) as file_count,
       SUM(object_size) as total_size_bytes,
       COUNT(DISTINCT backup_file) as backup_count
FROM catalog_table_name
WHERE year = '2025' AND month = '01'
GROUP BY backup_date
ORDER BY backup_date DESC;
```

#### Find Large Files in Backups

```sql
SELECT backup_file, object_name, object_size, backup_date
FROM catalog_table_name
WHERE object_size > 100000000  -- Files larger than 100MB
ORDER BY object_size DESC;
```

### Query Performance Optimization

- **Use Partitions**: Always include `year`, `month`, and `day` filters when possible
- **Limit Results**: Use `LIMIT` clause for large result sets
- **Index on Patterns**: Use `LIKE` patterns efficiently (avoid leading wildcards when possible)
- **Date Filtering**: Use date partitions instead of timestamp ranges for better performance

## File Recovery Process

### Identifying Files to Recover

1. **Query the Catalog**: Use Athena to find the backup file containing your data
2. **Note the Backup File**: Record the `backup_file` name from query results
3. **Locate the Archive**: Find the corresponding `.tar.zst` file in your backup bucket

### Manual Recovery Steps

#### Option 1: Download and Extract Locally

```bash
# Download the backup file
aws s3 cp s3://backup-bucket/path/backup_1753329108085.tar.zst ./

# Extract the archive (requires zstd and tar)
zstd -d backup_1753329108085.tar.zst
tar -tf backup_1753329108085.tar  # List contents
tar -xf backup_1753329108085.tar  # Extract all files

# Extract specific files only
tar -xf backup_1753329108085.tar objects/specific-file.pdf
```

#### Option 2: Programmatic Recovery

```python
import boto3
import tarfile
import zstandard as zstd

def recover_file_from_backup(backup_bucket, backup_key, target_file, output_path):
    """
    Recover a specific file from a backup archive.
    """
    s3 = boto3.client('s3')
    
    # Download backup file
    s3.download_file(backup_bucket, backup_key, 'temp_backup.tar.zst')
    
    # Decompress
    with open('temp_backup.tar.zst', 'rb') as compressed:
        dctx = zstd.ZstdDecompressor()
        with open('temp_backup.tar', 'wb') as decompressed:
            dctx.copy_stream(compressed, decompressed)
    
    # Extract specific file
    with tarfile.open('temp_backup.tar', 'r') as tar:
        tar.extract(f'objects/{target_file}', path=output_path)
    
    # Cleanup
    os.remove('temp_backup.tar.zst')
    os.remove('temp_backup.tar')

# Usage
recover_file_from_backup(
    'my-backup-bucket',
    'source-prefix/backup_1753329108085.tar.zst',
    'important-document.pdf',
    './recovered/'
)
```

## Monitoring and Maintenance

### CloudWatch Metrics

The system provides specific metrics for backup mode:

- **Backup Upload Success Rate**: Percentage of successful backup uploads
- **Catalog Write Success Rate**: Percentage of successful catalog entries
- **Backup File Size**: Size of compressed archives
- **Compression Ratio**: Effectiveness of compression for backup files

### Automated Maintenance

#### Glue Crawler Schedule

- **Frequency**: Runs daily at 2 AM UTC
- **Purpose**: Updates catalog schema and discovers new partitions
- **Manual Trigger**: Can be run manually from AWS Glue console when needed

#### Lifecycle Policies

The system automatically configures lifecycle policies:

- **Catalog Bucket**: Metadata files retained for 1 year
- **Query Results**: Athena query results retained for 30 days

### Troubleshooting

#### Common Issues

**Catalog Table Not Found**
- Ensure Glue crawler has run successfully
- Check crawler logs in CloudWatch
- Verify catalog bucket contains `.jsonl` files

**Query Performance Issues**
- Always use partition filters (`year`, `month`, `day`)
- Avoid queries without any filters on large datasets
- Use `LIMIT` clause for exploratory queries

**Missing Files in Catalog**
- Check if backup upload succeeded in target region logs
- Verify catalog write permissions for ECS tasks
- Run Glue crawler manually to update schema

**Backup Files Not Created**
- Verify `backup: true` flag is set correctly in configuration
- Check target region has backup destinations configured
- Review ECS task logs for backup processing errors

## Cost Considerations

### Storage Costs

- **Backup Mode**: Stores compressed archives (typically 30-70% smaller)
- **Normal Mode**: Stores individual decompressed files
- **Catalog Overhead**: Minimal metadata storage cost

### Compute Costs

- **Backup Mode**: Lower compute costs (no decompression in target)
- **Query Costs**: Athena charges per data scanned
- **Crawler Costs**: Minimal daily Glue crawler execution cost

### Cost Optimization Tips

1. **Use Appropriate Storage Classes**: Configure `storage_class` for backup destinations
2. **Partition Pruning**: Always use date filters in Athena queries
3. **Lifecycle Policies**: Configure appropriate retention for catalog data
4. **Compression Efficiency**: Monitor compression ratios to ensure effectiveness

## Security Considerations

### Access Control

- **Backup Buckets**: Standard S3 bucket policies apply
- **Catalog Access**: Controlled via IAM roles and Athena workgroup permissions
- **Query Permissions**: Users need Athena and Glue permissions for catalog access

### Encryption

- **At Rest**: Backup files encrypted with KMS (same as normal mode)
- **In Transit**: All data transfers use SSL/TLS
- **Catalog Data**: Metadata encrypted in S3 catalog bucket

### Compliance

- **Data Retention**: Configure lifecycle policies according to compliance requirements
- **Audit Trail**: All backup operations logged in CloudWatch
- **Access Logging**: Enable S3 access logging for backup buckets if required

## Best Practices

### Configuration

1. **Mixed Mode Strategy**: Use backup mode for archival, normal mode for active access
2. **Storage Class Selection**: Choose appropriate storage classes for backup destinations
3. **Prefix Organization**: Use consistent prefix structures for easier catalog navigation

### Querying

1. **Always Use Partitions**: Include date filters in all queries
2. **Limit Result Sets**: Use `LIMIT` for exploratory queries
3. **Index Common Patterns**: Design queries around common search patterns

### Maintenance

1. **Monitor Crawler**: Ensure daily crawler runs complete successfully
2. **Review Metrics**: Monitor backup success rates and compression ratios
3. **Test Recovery**: Periodically test file recovery procedures

### Performance

1. **Batch Size Optimization**: Monitor compression batch sizes for efficiency
2. **Query Optimization**: Use partition pruning and appropriate filters
3. **Resource Sizing**: Adjust ECS task resources based on backup file sizes

## Integration Examples

### Recovery Workflow

For automated recovery, follow this workflow:

1. **Query the catalog** using Athena to find the backup file
2. **Download the backup archive** from S3
3. **Extract the specific file** using zstd and tar
4. **Upload to destination** or use locally

Example workflow:

```bash
# Step 1: Query catalog (replace with actual table name)
aws athena start-query-execution \
    --query-string "SELECT backup_file FROM your_catalog_table WHERE object_name = 'target-file.pdf' LIMIT 1" \
    --work-group "your-workgroup"

# Step 2: Download backup file (after getting query results)
aws s3 cp s3://backup-bucket/path/backup_123456789.tar.zst ./

# Step 3: Extract specific file
zstd -d backup_123456789.tar.zst
tar -xf backup_123456789.tar objects/target-file.pdf
```

### Monitoring Dashboard

Create CloudWatch dashboard to monitor backup operations:

```json
{
    "widgets": [
        {
            "type": "metric",
            "properties": {
                "metrics": [
                    ["AWS/ECS", "BackupUploadSuccess", "ServiceName", "target-service"],
                    [".", "CatalogWriteSuccess", ".", "."],
                    [".", "CompressionRatio", ".", "."]
                ],
                "period": 300,
                "stat": "Average",
                "region": "us-east-1",
                "title": "Backup Operations"
            }
        }
    ]
}
```

## Conclusion

Backup mode provides a cost-effective solution for archival storage while maintaining searchability through the integrated catalog system. By storing compressed archives instead of individual files, you can achieve significant storage cost savings while retaining the ability to quickly locate and recover specific files when needed.

The combination of AWS Athena for querying and the automated catalog maintenance ensures that your backup data remains accessible and manageable at scale, making it an ideal solution for compliance, disaster recovery, and long-term data archival scenarios.