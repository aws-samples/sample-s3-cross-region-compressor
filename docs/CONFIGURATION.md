# Configuration Guide for S3 Cross-Region Compressor

This guide provides detailed information on configuring the S3 Cross-Region Compressor system.

## Configuration Files

The system uses two main configuration files located in the `configuration` directory:

1. `settings.json`: General settings and region configurations
2. `replication_config.json`: S3 bucket replication rules

## settings.json

The `settings.json` file configures the overall system settings including which AWS regions to use and their roles.

### File Structure

```json
{
    "stack_name": "s3-compressor",
    "enabled_regions": [
        {
            "region": "eu-west-1",
            "vpc_cidr": "10.100.0.0/16",
            "source_target": [ "source", "target" ],
            "availability_zones": 3
        },
        {
            "region": "us-east-2",
            "vpc_cidr": "10.102.0.0/16",
            "source_target": [ "source", "target" ],
            "availability_zones": 3
        },
        {
            "region": "ap-southeast-2",
            "vpc_cidr": "10.104.0.0/16",
            "source_target": [ "target" ],
            "availability_zones": 3
        }
    ],
    "notification_emails": [
        "user1@example.com",
        "user2@example.com"
    ],
    "tags": {
        "Environment": "dev",
        "Project": "s3-compressor"
    }
}
```

### Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `stack_name` | String | Base name for all CloudFormation stacks and resources created by the system |
| `enabled_regions` | Array | List of AWS regions where the system will be deployed |
| `notification_emails` | Array | List of email addresses to notify for CloudWatch alarms |
| `tags` | Object | AWS resource tags applied to all created resources, you can add additional Keys and Values |

#### enabled_regions parameters

Each entry in the `enabled_regions` array configures a specific AWS region:

| Parameter | Type | Description |
|-----------|------|-------------|
| `region` | String | AWS region code (e.g., `eu-west-1`) |
| `vpc_cidr` | String | CIDR block for the VPC created in this region (must not overlap with other VPCs) |
| `source_target` | Array | Role(s) for this region - can include `"source"`, `"target"`, or both |
| `availability_zones` | Number | Number of availability zones to use (1-6) |

The `source_target` parameter is particularly important:
- `"source"`: Deploys source components that monitor source S3 buckets and compress objects
- `"target"`: Deploys target components that receive and decompress objects
- `["source", "target"]`: Deploys both components in the same region

### Example Configurations

#### Multi-Region with Separate Source and Target

```json
{
    "stack_name": "s3-compressor",
    "enabled_regions": [
        {
            "region": "eu-west-1",
            "vpc_cidr": "10.100.0.0/16",
            "source_target": [ "source" ],
            "availability_zones": 3
        },
        {
            "region": "us-east-2",
            "vpc_cidr": "10.102.0.0/16",
            "source_target": [ "target" ],
            "availability_zones": 3
        }
    ],
    "tags": {
        "Environment": "production",
        "Project": "s3-compressor"
    }
}
```

#### Multiple Target Regions

```json
{
    "stack_name": "s3-compressor",
    "enabled_regions": [
        {
            "region": "eu-west-1",
            "vpc_cidr": "10.100.0.0/16",
            "source_target": [ "source" ],
            "availability_zones": 3
        },
        {
            "region": "us-east-2",
            "vpc_cidr": "10.102.0.0/16",
            "source_target": [ "target" ],
            "availability_zones": 3
        },
        {
            "region": "ap-southeast-2",
            "vpc_cidr": "10.104.0.0/16",
            "source_target": [ "target" ],
            "availability_zones": 3
        }
    ],
    "tags": {
        "Environment": "production",
        "Project": "global-distribution"
    }
}
```

#### Bidirectional Replication

```json
{
    "stack_name": "s3-compressor",
    "enabled_regions": [
        {
            "region": "eu-west-1",
            "vpc_cidr": "10.100.0.0/16",
            "source_target": [ "source", "target" ],
            "availability_zones": 3
        },
        {
            "region": "us-east-2",
            "vpc_cidr": "10.102.0.0/16",
            "source_target": [ "source", "target" ],
            "availability_zones": 3
        }
    ],
    "tags": {
        "Environment": "production",
        "Project": "bi-directional"
    }
}
```

## replication_config.json

The `replication_config.json` file defines which S3 buckets should be monitored for objects, how they should be processed, and where the compressed objects should be sent.

### File Structure

```json
{
    "replication_configuration": [
        {
            "source": {
                "region": "eu-west-1",
                "bucket": "source-bucket-name",
                "kms_key_arn": "arn:aws:kms:region:account:key/keyid",
                "prefix_filter": "folder1/",
                "suffix_filter": ".jpg",
                "scaling_limit": 30,
                "cpu": 2048,
                "memory": 4096,
                "scaling_target_backlog_per_task": 60
            },
            "destinations": [
                {
                    "region": "us-east-2",
                    "bucket": "target-bucket-name",
                    "kms_key_arn": ""
                },
                {
                    "region": "ap-southeast-2",
                    "bucket": "target-bucket-name-ap",
                    "kms_key_arn": "arn:aws:kms:ap-southeast-2:account:key/keyid"
                }
            ]
        }
    ]
}
```

### Parameters

#### Source Configuration

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `region` | String | Yes | AWS region of the source bucket (must be configured as "source" in settings.json) |
| `bucket` | String | Yes | S3 bucket name to monitor for new objects |
| `kms_key_arn` | String | No | KMS key ARN for encrypted source objects (can be omitted or left empty if not using KMS) |
| `prefix_filter` | String | No | Prefix to filter objects (e.g., "folder1/") - can be omitted or left empty if no prefix filtering is needed |
| `suffix_filter` | String | No | Suffix to filter objects (e.g., ".jpg") - can be omitted or left empty if no suffix filtering is needed |
| `scaling_limit` | Number | No | Maximum number of ECS tasks (1-1000, default: 20) |
| `cpu` | Number | No | ECS task CPU units (256-16384, default: 2048) * |
| `memory` | Number | No | ECS task memory in MB (512-122880, default: 4096) * |
| `ephemeral_storage` | Number | No | ECS task ephemeral storage in GiB (21-200, default: 20). Increase this value when processing large files or when the task requires more temporary storage. |
| `scaling_target_backlog_per_task` | Number | No | Queue messages per task for auto-scaling (default: 60) |
| `visibility_timeout` | Number | No | SQS visibility timeout in seconds for message processing (default: 300). Increase this value for processing large objects to prevent duplicate processing. |

* CPU and Memory sizing needs to be compliant with [Fargate Task Sizing requirements.](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task_definition_parameters.html#task_size)

#### Destination Configuration

Each entry in the `destinations` array configures a specific target bucket:

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `region` | String | Yes | AWS region of the target bucket (must be configured as "target" in settings.json) |
| `bucket` | String | Yes | S3 bucket name to store decompressed objects |
| `kms_key_arn` | String | No | KMS key ARN for encryption in target region (can be omitted or left empty if not using KMS) |
| `storage_class` | String | No | S3 storage class for target objects (Only: "INTELLIGENT_TIERING", "STANDARD", "STANDARD_IA", "GLACIER_IR", "ONEZONE_IA", "GLACIER" and "DEEP_ARCHIVE") - if omitted, the original storage class from the source object will be used |
| `backup` | Boolean | No | Set to `true` to enable backup mode for this destination (stores compressed archives instead of individual files, default: `false`) - see [BACKUP_MODE.md](BACKUP_MODE.md) for details |

### Example Configurations

#### Basic Configuration

```json
{
    "replication_configuration": [
        {
            "source": {
                "region": "eu-west-1",
                "bucket": "my-source-bucket",
                "prefix_filter": "",
                "suffix_filter": "",
                "scaling_limit": 20
            },
            "destinations": [
                {
                    "region": "us-east-2",
                    "bucket": "my-target-bucket",
                    "kms_key_arn": ""
                }
            ]
        }
    ]
}
```

#### Multiple Source Buckets

```json
{
    "replication_configuration": [
        {
            "source": {
                "region": "eu-west-1",
                "bucket": "source-bucket-images",
                "prefix_filter": "",
                "suffix_filter": ".jpg",
                "scaling_limit": 20
            },
            "destinations": [
                {
                    "region": "us-east-2",
                    "bucket": "target-bucket-images",
                    "kms_key_arn": ""
                }
            ]
        },
        {
            "source": {
                "region": "eu-west-1",
                "bucket": "source-bucket-documents",
                "prefix_filter": "",
                "suffix_filter": ".pdf",
                "scaling_limit": 10
            },
            "destinations": [
                {
                    "region": "us-east-2",
                    "bucket": "target-bucket-documents",
                    "kms_key_arn": ""
                }
            ]
        }
    ]
}
```

#### Filtering with Prefix and Suffix

```json
{
    "replication_configuration": [
        {
            "source": {
                "region": "eu-west-1",
                "bucket": "my-source-bucket",
                "prefix_filter": "images/",
                "suffix_filter": ".jpg",
                "scaling_limit": 20
            },
            "destinations": [
                {
                    "region": "us-east-2",
                    "bucket": "my-target-bucket",
                    "kms_key_arn": ""
                }
            ]
        }
    ]
}
```

#### Multiple Destinations

```json
{
    "replication_configuration": [
        {
            "source": {
                "region": "eu-west-1",
                "bucket": "my-source-bucket",
                "prefix_filter": "",
                "suffix_filter": "",
                "scaling_limit": 30
            },
            "destinations": [
                {
                    "region": "us-east-2",
                    "bucket": "us-target-bucket",
                    "kms_key_arn": ""
                },
                {
                    "region": "ap-southeast-2",
                    "bucket": "ap-target-bucket",
                    "kms_key_arn": ""
                }
            ]
        }
    ]
}
```

#### Storage Class and KMS Configuration

```json
{
    "replication_configuration": [
        {
            "source": {
                "region": "eu-west-1",
                "bucket": "my-source-bucket",
                "prefix_filter": "",
                "suffix_filter": "",
                "scaling_limit": 30
            },
            "destinations": [
                {
                    "region": "us-east-2",
                    "bucket": "us-target-bucket",
                    "storage_class": "STANDARD_IA"
                },
                {
                    "region": "ap-southeast-2",
                    "bucket": "ap-target-bucket",
                    "kms_key_arn": "arn:aws:kms:ap-southeast-2:account:key/keyid"
                },
                {
                    "region": "eu-central-1",
                    "bucket": "eu-archive-bucket",
                    "storage_class": "GLACIER_IR",
                    "kms_key_arn": "arn:aws:kms:eu-central-1:account:key/keyid"
                }
            ]
        }
    ]
}
```

#### Backup Mode Configuration

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
                    "bucket": "backup-archive-bucket",
                    "storage_class": "GLACIER_IR",
                    "backup": true
                },
                {
                    "region": "ca-central-1",
                    "bucket": "active-files-bucket",
                    "storage_class": "STANDARD"
                }
            ]
        }
    ]
}
```

In this example:
- **eu-central-1**: Receives compressed archives (backup mode) for cost-effective archival
- **ca-central-1**: Receives individual decompressed files for immediate access

For complete backup mode documentation, see [BACKUP_MODE.md](BACKUP_MODE.md).

**Important**: When backup mode is enabled, the system creates a catalog bucket for metadata storage. This bucket retains metadata files indefinitely and will persist even if the CDK stack is destroyed, ensuring backup metadata is preserved for compliance and recovery purposes.

#### Configuration with Omitted Optional Parameters

```json
{
    "replication_configuration": [
        {
            "source": {
                "region": "eu-west-1",
                "bucket": "my-source-bucket",
                "scaling_limit": 30
                /* kms_key_arn, prefix_filter, and suffix_filter are omitted */
            },
            "destinations": [
                {
                    "region": "us-east-2",
                    "bucket": "us-target-bucket"
                    /* kms_key_arn is omitted */
                }
            ]
        }
    ]
}
```

#### Configuration with Visibility Timeout for Large Files

```json
{
    "replication_configuration": [
        {
            "source": {
                "region": "eu-west-1",
                "bucket": "my-source-bucket",
                "prefix_filter": "large-files/",
                "visibility_timeout": 900,  // 15 minutes for large files
                "cpu": 4096,
                "memory": 8192
            },
            "destinations": [
                {
                    "region": "us-east-2",
                    "bucket": "us-target-bucket"
                }
            ]
        },
        {
            "source": {
                "region": "eu-west-1",
                "bucket": "my-source-bucket",
                "prefix_filter": "small-files/",
                "visibility_timeout": 300  // Default 5 minutes is fine for small files
            },
            "destinations": [
                {
                    "region": "us-east-2",
                    "bucket": "us-target-bucket"
                }
            ]
        }
    ]
}
```

#### Configuration with Ephemeral Storage for Large Files

```json
{
    "replication_configuration": [
        {
            "source": {
                "region": "eu-west-1",
                "bucket": "my-source-bucket",
                "prefix_filter": "very-large-files/",
                "visibility_timeout": 1800,
                "cpu": 4096,
                "memory": 16384,
                "ephemeral_storage": 100  // 100 GiB of ephemeral storage for temporary files
            },
            "destinations": [
                {
                    "region": "us-east-2",
                    "bucket": "us-target-bucket"
                }
            ]
        }
    ]
}
```

Note: The comment lines in the example above are for illustration only. In a real JSON file, you should not include comments.

## Best Practices

### VPC CIDR Allocation

- VPC has no connectivity between them or to the rest of your infrastructure, still try to avoid CIDRs from overlapping
- Allocate sufficient IP space based on your Max scaling settings workload
    - For example 10 source buckets, each with max scaling up to 500 tasks is equal to at least 5000 IPs + VPC Endpoints

### Configuration optimization

- Make sure you use the 'destinations' configuration as a List if you need to fan out data from the same source
- This will ensure the compression process happens only once, and replicated to all your target regions
- The solution has individual queues for each source, so there is no noise neighbor there. But S3 Replication used in the outbound bucket is prioritized. We recommend putting higher priority sources higher in the configuration file

### Scaling Configuration

- Set `scaling_limit` based on expected workload and object sizes
- For large objects (>1GB), use higher CPU/memory settings
- For many small objects, increase `scaling_target_backlog_per_task`

### Prefix/Suffix Filtering

- Use suffix filters for specific file types
- Combine both for granular filtering
- Example: `prefix_filter`: "images/2023/" and `suffix_filter`: ".jpg"

### Performance Tuning

- CPU and memory settings affect compression speed
- Higher values improve processing time but cost more
- Recommended starting point: 2048 CPU units, 4096 MB memory
- Adjust based on CloudWatch metrics

### SQS Visibility Timeout

- The `visibility_timeout` parameter determines how long (in seconds) a message remains invisible to other consumers after being retrieved
- Default value is 300 seconds (5 minutes), which is sufficient for most workloads
- For large objects or when processing many objects in a single batch, consider increasing this value
- Recommended values based on object size:
  - Standard files (<100MB): 300 seconds (default)
  - Large files (100MB-1GB): 600-900 seconds (10-15 minutes)
  - Very large files (>1GB): 1200-1800 seconds (20-30 minutes)
- If you observe duplicate processing of the same objects, this is a sign that your visibility timeout is too short
- Setting visibility timeout too high can delay processing of messages if a task fails

### Storage Class Management

- By default, the system preserves the original storage class of S3 objects when they are replicated to target regions
- You can override this behavior by specifying a `storage_class` parameter for any target bucket in the `destinations` array
- This allows you to implement cost-saving strategies like:
  - Moving infrequently accessed data to `STANDARD_IA` or `INTELLIGENT_TIERING` in disaster recovery regions
  - Archiving older data to `GLACIER_IR` or `DEEP_ARCHIVE` in compliance regions
  - Keeping frequently accessed data in `STANDARD` storage in primary regions
- Storage class configuration can be combined with KMS encryption for region-specific data protection policies
- If no `storage_class` is specified, the original storage class of the source object will be preserved
- Different destinations can have different storage classes for the same replicated objects

### Ephemeral Storage Configuration

- The `ephemeral_storage` parameter configures the amount of temporary storage available to ECS Fargate tasks
- Default value is 20 GiB (AWS Fargate default), which is sufficient for most workloads
- Valid values range from 21 to 200 GiB (values of 20 or less will use the default 20 GiB)
- Consider increasing this value when:
  - Processing very large files (especially those >1GB)
  - Working with files that expand significantly during decompression/compression operations
  - Experiencing "no space left on device" errors in task logs
- The solution will batch 10 messages, and spike at double the size of the input, consider this to define the correct value
  - < 1GB the default 20GB is typically enough
  - 2GB per source Object x 10 x 2 then increase the size to 40+ GB
- Using more ephemeral storage will increase the cost of your Fargate tasks
- Always monitor CloudWatch metrics to determine if your tasks are constrained by storage
