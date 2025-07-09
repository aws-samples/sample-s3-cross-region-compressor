# Security Guide for S3 Cross-Region Compressor

This guide provides detailed information about the security features, configurations, and best practices for the S3 Cross-Region Compressor system.

## Overview

The S3 Cross-Region Compressor is designed with security as a fundamental principle. The architecture implements multiple layers of security controls across network, data, identity, and operational domains to ensure that your data remains secure throughout the compression and replication processes.

## Network Security

### VPC Isolation

All components of the system run in private subnets within isolated VPCs:

- **Private Subnet Configuration**: All ECS tasks run in `PRIVATE_ISOLATED` subnets with no internet gateway or NAT gateway access
- **Region-Specific VPCs**: Each region has its own isolated VPC with non-overlapping CIDR blocks
- **Multi-AZ Deployment**: Resources are distributed across multiple availability zones for resilience

### VPC Endpoints

The system uses VPC Endpoints to access AWS services without traversing the public internet:

- **Gateway Endpoints**:
  - S3 for object storage access
  - DynamoDB for compression settings storage

- **Interface Endpoints**:
  - Amazon ECR for container image access
  - CloudWatch Logs for logging
  - SQS for message queuing
  - DynamoDB for configuration storage

### Network Access Controls

- **Security Groups**: ECS tasks use security groups that only allow outbound traffic
- **No Inbound Access**: No inbound rules are configured, blocking all incoming connections
- **No Internet Access**: Components have no route to the internet, preventing data exfiltration

### Data Transfer Security

- **AWS Backbone Network**: Cross-region replication uses AWS's private network backbone
- **TLS Encryption**: All API calls and data transfers use TLS encryption in transit

## Data Security

### Encryption at Rest

The system implements comprehensive encryption at rest using AWS KMS:

- **S3 Bucket Encryption**: 
  - All solution-managed buckets enforce KMS encryption
  - Bucket key feature enabled for reduced API calls and costs
  - User-provided buckets can optionally use KMS encryption

- **KMS Key Configuration**:
  - Region-specific KMS keys with annual rotation enabled
  - Separate keys for each function (source processing, target processing)
  - IAM-based access control to encryption keys

### Cross-Region Encryption Considerations

- **Key Separation**: Each region uses its own KMS keys
- **Key Permissions**: S3 replication roles have permissions to decrypt in source region and encrypt in target regions
- **Manifest Encryption**: The manifest file containing metadata is encrypted along with the objects

### Object Integrity

- **Metadata Preservation**: Original object metadata and tags are preserved through the compression/decompression process
- **Manifest Validation**: Target processing validates manifests before restoring objects
- **Additional Tags for Consistency**: The solution replicates all the source tags adds 2 additional tags to the target objects:
    - OriginalCreationTime: datetime value for original object put in source
    - OriginalETag: uniquely identifiable ID for original object put in source

### Storage Security

- **Public Access Blocking**: All S3 buckets have Block Public Access enabled at the bucket level
- **SSL Enforcement**: HTTPS connections are required for all S3 operations
- **Lifecycle Management**: Automated cleanup of temporary objects after successful replication

## Identity and Access Management

### IAM Role Design

The system implements the principle of least privilege through specialized IAM roles:

- **ECS Task Roles**: 
  - Task-specific permissions for source and target processing
  - Separate roles for each deployment configuration
  - Permissions scoped to specific resources and actions

- **ECS Execution Roles**: 
  - Permissions limited to pulling container images and writing logs
  - No access to customer data

- **S3 Replication Roles**:
  - Limited to source bucket read access and destination bucket write access
  - KMS decrypt/encrypt permissions for cross-region replication

### Authentication Mechanisms

- **IAM Role-Based**: All components use IAM roles without long-term credentials
- **Short-Lived Credentials**: Temporary credentials via IAM roles for ECS tasks
- **No Embedded Secrets**: No credentials or secrets are stored in containers or configuration files

## Operational Security

### Logging and Audit

- **CloudWatch Logs**: All container logs are captured in CloudWatch
- **Structured Logging**: JSON-formatted logs for easier analysis and filtering
- **Log Retention**: Configurable log retention periods based on compliance needs (one month default)

### Resource Governance

- **Resource Tagging**: All resources tagged for governance and cost allocation
- **IAM Access Analyzer**: Compatible with AWS IAM Access Analyzer for permission verification
- **AWS Config**: Compatible with AWS Config for compliance monitoring

### Container Security

- **Minimal Base Images**: Container images based on minimal Linux distributions (python:3.13.5-slim)
- **Non-Root User Implementation**: Containers run as `appuser` with UID/GID 1000
  - Dedicated user and group created for application execution
  - Proper directory ownership and permissions (755 for directories, 644 for files)
  - Enhanced security through principle of least privilege
  - Compatible with Kubernetes Pod Security Standards
- **Enhanced Directory Structure**: 
  - `/app` (755, appuser:appuser) - Application code directory
  - `/tmp/app-temp` (1777, appuser:appuser) - Primary temporary directory
  - `/tmp/app-work` (1777, appuser:appuser) - Alternative temporary directory
  - Environment variables configured for proper temp directory usage
- **Image Scanning**: Container images can be scanned for vulnerabilities through ECR
- **No Persistent Storage**: Containers are stateless with no persistent storage

## Additional Security Resources

- [AWS S3 Security Best Practices](https://docs.aws.amazon.com/AmazonS3/latest/userguide/security-best-practices.html)
- [AWS KMS Best Practices](https://docs.aws.amazon.com/kms/latest/developerguide/best-practices.html)
- [ECS Security Best Practices](https://docs.aws.amazon.com/AmazonECS/latest/bestpracticesguide/security.html)
- [VPC Security Best Practices](https://docs.aws.amazon.com/vpc/latest/userguide/vpc-security-best-practices.html)

## Security Updates and Patching

- Container images should be regularly rebuilt to incorporate security patches
- AWS service updates are automatically applied by AWS
- CDK dependencies should be kept updated to resolve security vulnerabilities
