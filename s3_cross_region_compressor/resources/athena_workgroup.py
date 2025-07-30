"""
AWS Athena resources for querying catalog metadata.
"""

from aws_cdk import (
    RemovalPolicy,
    Duration,
    aws_athena as athena,
    aws_s3 as s3,
    aws_iam as iam,
)
from constructs import Construct


def create_athena_query_results_bucket(scope: Construct, kms_key, stack_name: str) -> s3.Bucket:
    """
    Create S3 bucket for Athena query results.
    
    Args:
        scope: CDK construct scope
        kms_key: KMS key for encryption
        stack_name: Stack name for bucket naming
        
    Returns:
        s3.Bucket: The created query results bucket
    """
    bucket = s3.Bucket(
        scope,
        'athena-query-results',
        bucket_name=f'{stack_name}-{scope.account}-{scope.region}-athena-results',
        encryption=s3.BucketEncryption.KMS,
        encryption_key=kms_key,
        versioned=False,
        removal_policy=RemovalPolicy.DESTROY,
        auto_delete_objects=True,
        enforce_ssl=True,
        lifecycle_rules=[
            s3.LifecycleRule(
                id='DeleteQueryResults',
                enabled=True,
                expiration=Duration.days(30),  # Keep query results for 30 days
            )
        ]
    )
    
    return bucket


def create_athena_workgroup(
    scope: Construct,
    query_results_bucket: s3.Bucket,
    stack_name: str
) -> athena.CfnWorkGroup:
    """
    Create Athena workgroup for catalog queries.
    
    Args:
        scope: CDK construct scope
        query_results_bucket: S3 bucket for query results
        stack_name: Stack name for workgroup naming
        
    Returns:
        athena.CfnWorkGroup: The created Athena workgroup
    """
    workgroup = athena.CfnWorkGroup(
        scope,
        'CatalogWorkGroup',
        name=f'{stack_name}-catalog-workgroup',
        description='Workgroup for querying S3 backup catalog metadata',
        work_group_configuration=athena.CfnWorkGroup.WorkGroupConfigurationProperty(
            result_configuration=athena.CfnWorkGroup.ResultConfigurationProperty(
                output_location=f's3://{query_results_bucket.bucket_name}/query-results/',
                encryption_configuration=athena.CfnWorkGroup.EncryptionConfigurationProperty(
                    encryption_option='SSE_KMS',
                    kms_key=query_results_bucket.encryption_key.key_arn
                )
            ),
            enforce_work_group_configuration=True,
            publish_cloud_watch_metrics_enabled=True,
            bytes_scanned_cutoff_per_query=1000000000,  # 1GB limit per query
        )
    )
    
    return workgroup


def create_athena_user_role(
    scope: Construct,
    catalog_bucket: s3.Bucket,
    query_results_bucket: s3.Bucket,
    database_name: str
) -> iam.Role:
    """
    Create IAM role for Athena users.
    
    Args:
        scope: CDK construct scope
        catalog_bucket: S3 catalog bucket
        query_results_bucket: S3 query results bucket
        database_name: Glue database name
        
    Returns:
        iam.Role: The created IAM role for Athena users
    """
    role = iam.Role(
        scope,
        'AthenaUserRole',
        assumed_by=iam.AccountRootPrincipal(),
        description='Role for users to query catalog metadata with Athena'
    )
    
    # Grant permissions to read catalog data
    catalog_bucket.grant_read(role)
    
    # Grant permissions to write query results
    query_results_bucket.grant_read_write(role)
    
    # Grant Glue permissions
    role.add_to_policy(
        iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=[
                'glue:GetDatabase',
                'glue:GetDatabases',
                'glue:GetTable',
                'glue:GetTables',
                'glue:GetPartition',
                'glue:GetPartitions'
            ],
            resources=[
                f'arn:aws:glue:{scope.region}:{scope.account}:catalog',
                f'arn:aws:glue:{scope.region}:{scope.account}:database/{database_name}',
                f'arn:aws:glue:{scope.region}:{scope.account}:table/{database_name}/*'
            ]
        )
    )
    
    # Grant Athena permissions
    role.add_to_policy(
        iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=[
                'athena:BatchGetQueryExecution',
                'athena:GetQueryExecution',
                'athena:GetQueryResults',
                'athena:GetWorkGroup',
                'athena:StartQueryExecution',
                'athena:StopQueryExecution'
            ],
            resources=['*']
        )
    )
    
    return role